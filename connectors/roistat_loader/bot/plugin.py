plugin = {
    "menu_callback": "command_roistat_db_manager",
    "router": "connectors.roistat_loader.bot.handlers:router_roistat",
    "keyboard": "connectors.roistat_loader.bot.keyboards:get_kb_roistat",
    "alpha_upload_callback": "command_upload_roistat_data",
}
