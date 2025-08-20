from __future__ import annotations

import asyncio
import os
from typing import Any

import capnp
from stardust import connect


async def main_async() -> None:
    """Populate a small movies graph using the local Stardust server.

    Defaults to connecting over the UNIX socket at unix:/tmp/stardust.sock.
    Override with the STARDUST_URL env var (e.g. "tcp://127.0.0.1:4000").
    """
    url = os.environ.get("STARDUST_URL", "unix:/tmp/stardust.sock")
    print(f"Connecting to Stardust at {url} ...")
    client = await connect(url)

    print("Creating nodes ...")
    # People
    actors: dict[str, int] = {}
    for name in [
        "Keanu Reeves",
        "Carrie-Anne Moss",
        "Laurence Fishburne",
        "Hugo Weaving",
        "Ana de Armas",
    ]:
        res = await client.create_node(labels=["Person", "Actor"], hot_props={"name": name})
        actors[name] = int(res["node"]["id"])  # type: ignore[index]

    directors: dict[str, int] = {}
    for name in [
        "Lana Wachowski",
        "Lilly Wachowski",
        "Chad Stahelski",
    ]:
        res = await client.create_node(labels=["Person", "Director"], hot_props={"name": name})
        directors[name] = int(res["node"]["id"])  # type: ignore[index]

    # Movies
    movies: dict[str, int] = {}
    movie_defs: list[tuple[str, int]] = [
        ("The Matrix", 1999),
        ("The Matrix Reloaded", 2003),
        ("John Wick", 2014),
        ("Blade Runner 2049", 2017),
    ]
    for title, year in movie_defs:
        res = await client.create_node(
            labels=["Movie"],
            hot_props={"title": title, "year": year},
        )
        movies[title] = int(res["node"]["id"])

    print("Creating relationships ...")
    # ACTED_IN
    acted_in: list[tuple[str, str]] = [
        ("Keanu Reeves", "The Matrix"),
        ("Carrie-Anne Moss", "The Matrix"),
        ("Laurence Fishburne", "The Matrix"),
        ("Hugo Weaving", "The Matrix"),
        ("Keanu Reeves", "John Wick"),
        ("Ana de Armas", "Blade Runner 2049"),
    ]
    for actor_name, movie_title in acted_in:
        _ = await client.add_edge(
            src=actors[actor_name],
            dst=movies[movie_title],
            edge_type="ACTED_IN",
            props={},
        )

    # DIRECTED
    directed_by: list[tuple[str, str]] = [
        ("Lana Wachowski", "The Matrix"),
        ("Lilly Wachowski", "The Matrix"),
        ("Chad Stahelski", "John Wick"),
    ]
    for director_name, movie_title in directed_by:
        _ = await client.add_edge(
            src=directors[director_name],
            dst=movies[movie_title],
            edge_type="DIRECTED",
            props={},
        )

    for node in movies.values():
        header: dict[str, Any] = await client.get_node(node)
        print(header)
        print(await client.list_adjacency(node))


def main() -> None:
    asyncio.run(capnp.run(main_async()))
