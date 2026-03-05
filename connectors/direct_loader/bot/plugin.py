plugin = {
    "menu_callback": "command_direct_db_manager",
    "router": "connectors.direct_loader.bot.handlers:router_direct",
    "keyboard": "connectors.direct_loader.bot.keyboards:get_kb_direct",
    "alpha_upload_callback": "command_upload_yd_data",
    "admin": {
        "button_text": "💾 Выгрузка Директа",
        "button_callback": "admin_upload_all_direct",
        "router": "connectors.direct_loader.bot.admin.handlers:router_direct_admin",
    },
}
