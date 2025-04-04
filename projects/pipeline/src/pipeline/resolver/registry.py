from collections.abc import Callable

from pipeline.resolver.common import FileStoreEntry, FileStoreDataFrame


class FileMatchRegistry:
    def __init__(self):
        self.registry = {}

    def register(self, file_type: str):
        assert file_type not in self.registry, f"File resolver for {file_type} is already registered."

        def wrapper(
            func: Callable[[FileStoreEntry, FileStoreDataFrame], list[FileStoreEntry]],
        ) -> Callable[[FileStoreEntry, FileStoreDataFrame], list[FileStoreEntry]]:
            self.registry[file_type] = func
            return func

        return wrapper

    def get_matches(
        self, file_type: str, input_entry: FileStoreEntry, file_store: FileStoreDataFrame
    ) -> list[FileStoreEntry]:
        assert file_type in self.registry
        func = self.registry[file_type]
        return func(input_entry, file_store)

    def get_match(self, file_type: str, primary: FileStoreEntry, file_store: FileStoreDataFrame) -> FileStoreEntry:
        matches = self.get_matches(file_type, primary, file_store)
        assert matches, f"No matches found for {file_type} for primary file {primary.model_dump_json(indent=2)}"
        return matches[0]


file_match_registry = FileMatchRegistry()
