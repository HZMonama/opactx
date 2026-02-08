from __future__ import annotations

from typing import Any

CONTEXT_SCHEMA_DSL_META_SCHEMA: dict[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": "https://opactx.dev/schema/context.schema.dsl/v1",
    "title": "opactx context.schema.yaml DSL (v1)",
    "type": "object",
    "additionalProperties": False,
    "required": ["dsl", "id", "title", "description", "root", "schema"],
    "properties": {
        "dsl": {"const": "opactx.schema/v1"},
        "id": {"type": "string", "minLength": 1},
        "title": {"type": "string", "minLength": 1},
        "description": {"type": "string", "minLength": 1},
        "root": {"type": "string", "minLength": 1},
        "strict": {"type": "boolean"},
        "schema": {"$ref": "#/$defs/objectNode"},
        "definitions": {
            "type": "object",
            "additionalProperties": {"$ref": "#/$defs/node"},
            "propertyNames": {"minLength": 1},
        },
    },
    "$defs": {
        "common": {
            "type": "object",
            "properties": {
                "description": {"type": "string"},
                "required": {"type": "boolean"},
                "nullable": {"type": "boolean"},
                "default": {},
                "examples": {"type": "array"},
                "deprecated": {"type": "boolean"},
                "tags": {"type": "array", "items": {"type": "string"}},
            },
        },
        "refNode": {
            "type": "object",
            "additionalProperties": False,
            "required": ["$ref"],
            "properties": {
                "$ref": {"type": "string", "pattern": "^#/(definitions|\\$defs)/[^/]+$"},
                "description": {"type": "string"},
                "deprecated": {"type": "boolean"},
                "required": {"type": "boolean"},
            },
        },
        "objectNode": {
            "allOf": [
                {"$ref": "#/$defs/common"},
                {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["type"],
                    "properties": {
                        "type": {"const": "object"},
                        "description": {"type": "string"},
                        "required": {"type": "boolean"},
                        "nullable": {"type": "boolean"},
                        "default": {},
                        "examples": {"type": "array"},
                        "deprecated": {"type": "boolean"},
                        "tags": {"type": "array", "items": {"type": "string"}},
                        "fields": {
                            "type": "object",
                            "additionalProperties": {"$ref": "#/$defs/node"},
                            "propertyNames": {"minLength": 1},
                        },
                        "strict": {"type": "boolean"},
                        "allow_empty_object": {"type": "boolean"},
                    },
                },
            ]
        },
        "arrayNode": {
            "allOf": [
                {"$ref": "#/$defs/common"},
                {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["type", "items"],
                    "properties": {
                        "type": {"const": "array"},
                        "description": {"type": "string"},
                        "required": {"type": "boolean"},
                        "nullable": {"type": "boolean"},
                        "default": {},
                        "examples": {"type": "array"},
                        "deprecated": {"type": "boolean"},
                        "tags": {"type": "array", "items": {"type": "string"}},
                        "items": {"$ref": "#/$defs/node"},
                        "min_items": {"type": "integer", "minimum": 0},
                        "max_items": {"type": "integer", "minimum": 0},
                        "unique_by": {"type": "string", "minLength": 1},
                    },
                },
            ]
        },
        "stringNode": {
            "allOf": [
                {"$ref": "#/$defs/common"},
                {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["type"],
                    "properties": {
                        "type": {"const": "string"},
                        "description": {"type": "string"},
                        "required": {"type": "boolean"},
                        "nullable": {"type": "boolean"},
                        "default": {},
                        "examples": {"type": "array"},
                        "deprecated": {"type": "boolean"},
                        "tags": {"type": "array", "items": {"type": "string"}},
                        "min_len": {"type": "integer", "minimum": 0},
                        "max_len": {"type": "integer", "minimum": 0},
                        "pattern": {"type": "string"},
                        "enum": {"type": "array"},
                        "format": {"enum": ["date-time", "email", "uri", "uuid"]},
                    },
                },
            ]
        },
        "numberNode": {
            "allOf": [
                {"$ref": "#/$defs/common"},
                {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["type"],
                    "properties": {
                        "type": {"const": "number"},
                        "description": {"type": "string"},
                        "required": {"type": "boolean"},
                        "nullable": {"type": "boolean"},
                        "default": {},
                        "examples": {"type": "array"},
                        "deprecated": {"type": "boolean"},
                        "tags": {"type": "array", "items": {"type": "string"}},
                        "min": {"type": "number"},
                        "max": {"type": "number"},
                        "enum": {"type": "array"},
                    },
                },
            ]
        },
        "integerNode": {
            "allOf": [
                {"$ref": "#/$defs/common"},
                {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["type"],
                    "properties": {
                        "type": {"const": "integer"},
                        "description": {"type": "string"},
                        "required": {"type": "boolean"},
                        "nullable": {"type": "boolean"},
                        "default": {},
                        "examples": {"type": "array"},
                        "deprecated": {"type": "boolean"},
                        "tags": {"type": "array", "items": {"type": "string"}},
                        "min": {"type": "integer"},
                        "max": {"type": "integer"},
                        "enum": {"type": "array"},
                    },
                },
            ]
        },
        "booleanNode": {
            "allOf": [
                {"$ref": "#/$defs/common"},
                {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["type"],
                    "properties": {
                        "type": {"const": "boolean"},
                        "description": {"type": "string"},
                        "required": {"type": "boolean"},
                        "nullable": {"type": "boolean"},
                        "default": {},
                        "examples": {"type": "array"},
                        "deprecated": {"type": "boolean"},
                        "tags": {"type": "array", "items": {"type": "string"}},
                        "enum": {"type": "array"},
                    },
                },
            ]
        },
        "nullNode": {
            "allOf": [
                {"$ref": "#/$defs/common"},
                {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["type"],
                    "properties": {
                        "type": {"const": "null"},
                        "description": {"type": "string"},
                        "required": {"type": "boolean"},
                        "nullable": {"type": "boolean"},
                        "default": {},
                        "examples": {"type": "array"},
                        "deprecated": {"type": "boolean"},
                        "tags": {"type": "array", "items": {"type": "string"}},
                        "enum": {"type": "array"},
                    },
                },
            ]
        },
        "node": {
            "oneOf": [
                {"$ref": "#/$defs/refNode"},
                {"$ref": "#/$defs/objectNode"},
                {"$ref": "#/$defs/arrayNode"},
                {"$ref": "#/$defs/stringNode"},
                {"$ref": "#/$defs/numberNode"},
                {"$ref": "#/$defs/integerNode"},
                {"$ref": "#/$defs/booleanNode"},
                {"$ref": "#/$defs/nullNode"},
            ]
        },
    },
}
