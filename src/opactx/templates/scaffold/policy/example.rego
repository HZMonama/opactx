package example

default allow := false

allow {
  input.region == data.context.standards.allowed_regions[_]
}
