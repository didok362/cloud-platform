variable "nsg_name" {
  type = string
}

variable "location" {
  type = string
}

variable "resource_group_name" {
  type = string
}

variable "rules" {
  type = map(object({
    priority = number
    port     = string
  }))
}