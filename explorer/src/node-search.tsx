import { useState } from "react";

export const NodeSearch = ({
  onSearch,
}: {
  onSearch: (id: number) => void;
}) => {
  const [searchId, setSearchId] = useState("");

  return (
    <div className="absolute top-2 left-1/2 -translate-x-1/2 z-10">
      <input
        className="px-3 py-1 rounded bg-black/60 text-white placeholder-white/60 border border-white/30 focus:outline-none focus:ring-2 focus:ring-white/60"
        placeholder="Enter node id and press Enter"
        value={searchId}
        onChange={(e) => setSearchId(e.target.value)}
        onKeyDown={(e) => {
          if (e.key === "Enter") {
            const parsed = Number(searchId.trim());
            if (!Number.isNaN(parsed)) {
              onSearch(parsed);
            }
          }
        }}
        inputMode="numeric"
      />
    </div>
  );
};
