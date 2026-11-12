module "vps_firewall" {
  source = "../../modules/cloud-vps"

  nsg_name            = "messenger-vm-nsg"
  location            = "austriaeast"
  resource_group_name = "messenger-rg"

  rules = {
    HTTP = {
      priority = 300
      port     = "80"
    }
    HTTPS = {
      priority = 320
      port     = "443"
    }
    SSH = {
      priority = 340
      port     = "22"
    }
  }
}