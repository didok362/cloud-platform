output "vps_nsg_id" {
  description = "NSG ID from cloud-vps module"
  value       = module.vps_firewall.nsg_id
}