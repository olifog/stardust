from __future__ import annotations

import asyncio
import csv
import gzip
import os
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.request import urlretrieve

import capnp
from stardust import connect

# IMDb dataset sources
IMDB_TITLE_BASICS_URL = "https://datasets.imdbws.com/title.basics.tsv.gz"
IMDB_TITLE_PRINCIPALS_URL = "https://datasets.imdbws.com/title.principals.tsv.gz"
IMDB_NAME_BASICS_URL = "https://datasets.imdbws.com/name.basics.tsv.gz"
IMDB_TITLE_RATINGS_URL = "https://datasets.imdbws.com/title.ratings.tsv.gz"


@dataclass(frozen=True)
class Movie:
    tconst: str
    title: str
    year: int
    avg_rating: float
    num_votes: int
    genres: str | None
    runtime_minutes: int | None


@dataclass(frozen=True)
class Person:
    nconst: str
    name: str
    roles: frozenset[str]
    birth_year: int | None
    death_year: int | None
    primary_professions: tuple[str, ...]


@dataclass(frozen=True)
class Principal:
    nconst: str
    ordering: int
    category: str
    job: str | None
    characters: str | None


def _safe_text(value: str | None, max_len: int = 200) -> str:
    if not value:
        return ""
    if len(value) <= max_len:
        return value
    return value[: max_len - 1] + "…"


def _find_or_create_data_dir() -> Path:
    """Locate a `data` directory by walking up from CWD; create if missing.

    Preference order:
    - STARDUST_DATA_DIR env var
    - nearest ancestor directory named `data`
    - `data/` under current working directory
    """
    env_dir = os.environ.get("STARDUST_DATA_DIR")
    if env_dir:
        path = Path(env_dir).expanduser().resolve()
        path.mkdir(parents=True, exist_ok=True)
        return path
    dest = Path.cwd().resolve() / "data"
    dest.mkdir(parents=True, exist_ok=True)
    return dest


def _download_if_needed(url: str, dest: Path) -> Path:
    if dest.exists() and dest.stat().st_size > 0:
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    print(f"Downloading {url} -> {dest} ...")
    tmp_path = dest.with_suffix(dest.suffix + ".tmp")
    urlretrieve(url, tmp_path.as_posix())
    tmp_path.replace(dest)
    return dest


def _iter_tsv(path: Path) -> Iterable[list[str]]:
    with gzip.open(path, mode="rt", encoding="utf-8", newline="") as f:
        reader = csv.reader(f, delimiter="\t")
        # Skip header row
        header_skipped = False
        for row in reader:
            if not header_skipped:
                header_skipped = True
                continue
            yield row


def _load_ratings(ratings_path: Path) -> dict[str, tuple[float, int]]:
    """Return mapping tconst -> (averageRating, numVotes)."""
    result: dict[str, tuple[float, int]] = {}
    for row in _iter_tsv(ratings_path):
        # tconst, averageRating, numVotes
        if len(row) < 3:
            continue
        tconst, avg_str, votes_str = row
        try:
            avg = float(avg_str)
            votes = int(votes_str)
        except ValueError:
            continue
        result[tconst] = (avg, votes)
    return result


def _load_movies(basics_path: Path, ratings_path: Path, max_movies: int) -> list[Movie]:
    """Load eligible movies and return the top-N by popularity.

    Popularity heuristic: highest numVotes first, then averageRating, then most recent year.
    """
    ratings = _load_ratings(ratings_path)
    candidates: list[tuple[int, float, int, str, str, str | None, int | None]] = []
    # (numVotes, avgRating, year, tconst, title, genres, runtimeMinutes)

    for row in _iter_tsv(basics_path):
        # tconst, titleType, primaryTitle, originalTitle,
        # isAdult, startYear, endYear, runtimeMinutes, genres
        if len(row) < 9:
            continue
        (
            tconst,
            title_type,
            primary_title,
            _original_title,
            is_adult,
            start_year,
            _end_year,
            runtime_minutes,
            genres,
        ) = row[:9]
        if title_type != "movie" or is_adult == "1" or start_year == "\\N":
            continue
        try:
            year = int(start_year)
        except ValueError:
            continue

        avg, votes = ratings.get(tconst, (0.0, 0))
        rt: int | None
        try:
            rt = int(runtime_minutes) if runtime_minutes != "\\N" else None
        except ValueError:
            rt = None
        genres_str = None if genres == "\\N" else genres
        candidates.append((votes, avg, year, tconst, primary_title, genres_str, rt))

    # Sort by popularity: votes desc, rating desc, year desc
    candidates.sort(key=lambda x: (x[0], x[1], x[2]), reverse=True)
    top = candidates[:max_movies]
    return [
        Movie(
            tconst=t,
            title=title,
            year=yr,
            avg_rating=a,
            num_votes=v,
            genres=g,
            runtime_minutes=rt,
        )
        for v, a, yr, t, title, g, rt in top
    ]


def _load_principals(
    principals_path: Path,
    movie_ids: set[str],
) -> tuple[dict[str, list[Principal]], dict[str, list[Principal]]]:
    """Return mapping of tconst -> principals for actors and directors.

    Does not cap counts; preserves IMDb 'ordering' and captures 'characters' and 'job'.
    """
    actors_by_title: dict[str, list[Principal]] = defaultdict(list)
    directors_by_title: dict[str, list[Principal]] = defaultdict(list)
    for row in _iter_tsv(principals_path):
        # tconst, ordering, nconst, category, job, characters
        if len(row) < 6:
            continue
        tconst, ordering_str, nconst, category, job, characters = row
        if tconst not in movie_ids:
            continue
        try:
            ordering = int(ordering_str)
        except ValueError:
            ordering = 999999

        def _parse_characters(raw: str) -> str | None:
            if not raw or raw == "\\N":
                return None
            # often it's a JSON-like list string; take the first entry if possible
            s = raw.strip()
            if s.startswith("[") and s.endswith("]"):
                try:
                    import json

                    arr = json.loads(s)
                    if isinstance(arr, list) and arr:
                        first = arr[0]
                        return str(first)
                except (json.JSONDecodeError, TypeError, ValueError):
                    pass
            return s

        principal = Principal(
            nconst=nconst,
            ordering=ordering,
            category=category,
            job=None if job == "\\N" else job,
            characters=_parse_characters(characters),
        )

        if category in ("actor", "actress"):
            actors_by_title[tconst].append(principal)
        elif category == "director":
            directors_by_title[tconst].append(principal)

    # sort by IMDb ordering as a baseline
    for lst in actors_by_title.values():
        lst.sort(key=lambda p: p.ordering)
    for lst in directors_by_title.values():
        lst.sort(key=lambda p: p.ordering)

    return actors_by_title, directors_by_title


def _load_people(names_path: Path, nconsts: set[str]) -> dict[str, tuple[str, int | None, int | None, tuple[str, ...]]]:
    """Return mapping nconst -> (primaryName, birthYear, deathYear, professions)."""
    needed = set(nconsts)
    result: dict[str, tuple[str, int | None, int | None, tuple[str, ...]]] = {}
    if not needed:
        return result
    for row in _iter_tsv(names_path):
        # nconst, primaryName, birthYear, deathYear, primaryProfession, knownForTitles
        if len(row) < 6:
            continue
        nconst, primary_name, birth_year, death_year, primary_profession, _known_for = row[:6]
        if nconst in needed:
            by: int | None
            dy: int | None
            try:
                by = int(birth_year) if birth_year != "\\N" else None
            except ValueError:
                by = None
            try:
                dy = int(death_year) if death_year != "\\N" else None
            except ValueError:
                dy = None
            profs: tuple[str, ...] = tuple(
                p.strip() for p in (primary_profession.split(",") if primary_profession and primary_profession != "\\N" else []) if p.strip()
            )
            result[nconst] = (primary_name, by, dy, profs)
            if len(result) >= len(needed):
                break
    return result


async def _bounded_gather(
    coros: list[asyncio.Future[Any] | asyncio.Task[Any] | Any],
    limit: int = 50,
) -> list[Any]:
    semaphore = asyncio.Semaphore(limit)

    async def _wrap(coro: Any) -> Any:
        async with semaphore:
            return await coro

    return await asyncio.gather(*[_wrap(c) for c in coros])


async def main_async() -> None:
    """Populate a larger movies graph using IMDb datasets.

    Defaults to connecting over the UNIX socket at unix:/tmp/stardust.sock.
    Override with STARDUST_URL (e.g. "tcp://127.0.0.1:4000").

    Controls:
    - DEMO_MAX_MOVIES (default 900)
    - DEMO_MAX_PEOPLE (default 2100)
    """
    url = os.environ.get("STARDUST_URL", "unix:/tmp/stardust.sock")
    print(f"Connecting to Stardust at {url} ...")
    client: Any = await connect(url)

    data_dir = _find_or_create_data_dir()
    basics_gz = data_dir / "title.basics.tsv.gz"
    principals_gz = data_dir / "title.principals.tsv.gz"
    names_gz = data_dir / "name.basics.tsv.gz"
    ratings_gz = data_dir / "title.ratings.tsv.gz"

    _download_if_needed(IMDB_TITLE_BASICS_URL, basics_gz)
    _download_if_needed(IMDB_TITLE_PRINCIPALS_URL, principals_gz)
    _download_if_needed(IMDB_NAME_BASICS_URL, names_gz)
    _download_if_needed(IMDB_TITLE_RATINGS_URL, ratings_gz)

    max_movies = int(os.environ.get("DEMO_MAX_MOVIES", "900"))
    max_people = int(os.environ.get("DEMO_MAX_PEOPLE", "2100"))

    print(
        f"Loading up to {max_movies} most popular movies using {basics_gz} + ratings ..."
    )
    movies = _load_movies(basics_gz, ratings_gz, max_movies=max_movies)
    movie_ids = {m.tconst for m in movies}

    print("Loading principals (actors/directors) ...")
    actors_by_title, directors_by_title = _load_principals(
        principals_gz,
        movie_ids=movie_ids,
    )

    all_people_ids: set[str] = set()
    for principals in actors_by_title.values():
        all_people_ids.update(p.nconst for p in principals)
    for principals in directors_by_title.values():
        all_people_ids.update(p.nconst for p in principals)

    # If too many people, cap to top-N weighted by movie popularity
    if len(all_people_ids) > max_people:
        # Build movie popularity by numVotes
        votes_by_title: dict[str, int] = {}
        for row in _iter_tsv(ratings_gz):
            if len(row) < 3:
                continue
            tconst, _avg, votes_str = row
            try:
                votes_by_title[tconst] = int(votes_str)
            except ValueError:
                continue

        weight_by_person: dict[str, float] = defaultdict(float)
        for tconst, principals in actors_by_title.items():
            weight = float(votes_by_title.get(tconst, 0))
            if weight <= 0:
                weight = 1.0
            for p in principals:
                weight_by_person[p.nconst] += weight
        for tconst, principals in directors_by_title.items():
            weight = float(votes_by_title.get(tconst, 0))
            if weight <= 0:
                weight = 1.0
            for p in principals:
                weight_by_person[p.nconst] += 2.0 * weight  # weigh directors higher

        top_people = {
            n
            for n, _w in sorted(
                weight_by_person.items(), key=lambda kv: kv[1], reverse=True
            )[:max_people]
        }
        # Filter mappings to only include top people
        actors_by_title = {
            t: [p for p in principals if p.nconst in top_people]
            for t, principals in actors_by_title.items()
        }
        directors_by_title = {
            t: [p for p in principals if p.nconst in top_people]
            for t, principals in directors_by_title.items()
        }
        all_people_ids = top_people

    # Use all available actors and directors (no per-movie caps). Lists are already
    # filtered by top_people if the global cap applied; otherwise include everyone
    # present in the names dataset.
    selected_actors_by_title: dict[str, list[Principal]] = actors_by_title
    selected_directors_by_title: dict[str, list[Principal]] = directors_by_title

    print(f"Resolving {len(all_people_ids)} people names ...")
    nconst_to_details = _load_people(names_gz, all_people_ids)

    # Build Person structures with role sets
    person_roles: dict[str, set[str]] = defaultdict(set)
    for principals in selected_actors_by_title.values():
        for p in principals:
            person_roles[p.nconst].add("Actor")
    for principals in selected_directors_by_title.values():
        for p in principals:
            person_roles[p.nconst].add("Director")

    people: dict[str, Person] = {}
    for nconst in all_people_ids:
        details = nconst_to_details.get(nconst)
        if not details:
            continue
        name, birth_year, death_year, professions = details
        roles = frozenset(person_roles.get(nconst, set()) or {"Person"})
        people[nconst] = Person(
            nconst=nconst,
            name=name,
            roles=roles,
            birth_year=birth_year,
            death_year=death_year,
            primary_professions=professions,
        )

    print(f"Creating {len(movies)} movie nodes and {len(people)} person nodes ...")

    # Create movie nodes
    create_movie_tasks = [
        client.create_node(
            labels=["Movie"],
            hot_props={
                "title": _safe_text(m.title, 200),
                "year": m.year,
                "tconst": _safe_text(m.tconst, 32),
                "avgRating": m.avg_rating,
                "numVotes": m.num_votes,
                "genres": _safe_text(m.genres or "", 200),
                "runtimeMinutes": m.runtime_minutes if m.runtime_minutes is not None else 0,
            },
        )
        for m in movies
    ]
    movie_results = await _bounded_gather(create_movie_tasks, limit=50)
    movie_id_map: dict[str, int] = {}
    for m, res in zip(movies, movie_results, strict=False):
        movie_id_map[m.tconst] = int(res["node"]["id"])  # type: ignore[index]

    # Create person nodes
    create_person_tasks = []
    for person in people.values():
        labels = ["Person", *sorted(role for role in person.roles if role != "Person")]
        create_person_tasks.append(
            client.create_node(
                labels=labels,
                hot_props={
                    "name": _safe_text(person.name, 200),
                    "nconst": _safe_text(person.nconst, 32),
                    "birthYear": person.birth_year if person.birth_year is not None else 0,
                    "deathYear": person.death_year if person.death_year is not None else 0,
                    "primaryProfession": _safe_text(",".join(person.primary_professions), 200),
                },
            )
        )
    person_results = await _bounded_gather(create_person_tasks, limit=50)
    person_id_map: dict[str, int] = {}
    for person, res in zip(people.values(), person_results, strict=False):
        person_id_map[person.nconst] = int(res["node"]["id"])  # type: ignore[index]

    print("Creating relationships ...")
    edge_tasks = []
    for movie in movies:
        tconst = movie.tconst
        for principal in selected_actors_by_title.get(tconst, []):
            nconst = principal.nconst
            if nconst in person_id_map:
                edge_tasks.append(
                    client.add_edge(
                        src=person_id_map[nconst],
                        dst=movie_id_map[tconst],
                        edge_type="ACTED_IN",
                        props={
                            "role": _safe_text(principal.characters or "", 200),
                            "ordering": int(principal.ordering),
                            "category": _safe_text(principal.category, 32),
                        },
                    )
                )
        for principal in selected_directors_by_title.get(tconst, []):
            nconst = principal.nconst
            if nconst in person_id_map:
                edge_tasks.append(
                    client.add_edge(
                        src=person_id_map[nconst],
                        dst=movie_id_map[tconst],
                        edge_type="DIRECTED",
                        props={
                            "ordering": int(principal.ordering),
                            "job": _safe_text(principal.job or "", 200),
                        },
                    )
                )

    _ = await _bounded_gather(edge_tasks, limit=100)

    sample_count = min(5, len(movies))
    print(f"Done. Created {len(movies)} movies, {len(people)} people, and {len(edge_tasks)} edges.")
    print(f"Sample of {sample_count} movies with adjacency:")
    for movie in movies[:sample_count]:
        node_id = movie_id_map[movie.tconst]
        header: dict[str, Any] = await client.get_node(node_id)
        print(header)
        print(await client.list_adjacency(node_id))


def main() -> None:
    asyncio.run(capnp.run(main_async()))
