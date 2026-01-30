from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator


class Source(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    name: str
    type: str
    with_: dict[str, Any] = Field(default_factory=dict, alias="with")


class Transform(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    name: str
    type: str
    with_: dict[str, Any] = Field(default_factory=dict, alias="with")


class Output(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dir: str = "dist/bundle"
    include_policy: bool = False
    tarball: bool = False


class Config(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: str = "v1"
    schema_path: str = Field(default="schema/context.schema.json", alias="schema")
    context_dir: str = "context"
    sources: list[Source] = Field(default_factory=list)
    transforms: list[Transform] = Field(default_factory=list)
    output: Output = Field(default_factory=Output)

    @model_validator(mode="after")
    def _validate_config(self) -> "Config":
        if self.version != "v1":
            raise ValueError("Only version v1 is supported.")
        seen: set[str] = set()
        duplicates: set[str] = set()
        for source in self.sources:
            if source.name in seen:
                duplicates.add(source.name)
            seen.add(source.name)
        if duplicates:
            dup_list = ", ".join(sorted(duplicates))
            raise ValueError(f"Duplicate source names: {dup_list}")
        return self
