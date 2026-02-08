package example

default allow := false

allow {
  data.context.actor.role == "admin"
  input.action == data.context.request.action
  input.resource.type == data.context.request.resource.type
}
