output "nsg_id" {
  description = "ID of the Network Security Group"
  value       = azurerm_network_security_group.this.id
}

output "nsg_name" {
  description = "Name of the Network Security Group"
  value       = azurerm_network_security_group.this.name
}