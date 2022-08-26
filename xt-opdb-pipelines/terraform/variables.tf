variable "event_targets" {
  description = "Map of definitions for all event targets."
  type        = any
  default     = {}
}

variable "image_uri" {
  type    = string
  default = ""
}

variable "is_enabled" {
  description = "Whether the rule should be enabled."
  type        = bool
  default     = false
}

variable "octopus_tags" {
  description = "Octopus Tags"
  type = object({
    project         = string
    space           = string
    environment     = string
    project_group   = string
    release_channel = string
  })
  default = {
    environment = "value"
    project = "value"
    project_group = "value"
    release_channel = "value"
    space = "value"
  }
}

variable "policy" {
  description = "List of additional policies for Lambda access."
  type        = list(any)
  default     = []
}
