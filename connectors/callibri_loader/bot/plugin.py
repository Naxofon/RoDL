plugin = {
    "menu_callback": "command_callibri_db_manager",
    "router": "connectors.callibri_loader.bot.handlers:router_callibri",
    "keyboard": "connectors.callibri_loader.bot.keyboards:get_kb_callibri",
    "alpha_upload_callback": "command_upload_callibri_data",
    "admin": {
        "button_text": "💾 Выгрузка callibri",
        "button_callback": "admin_upload_all_callibri",
        "router": "connectors.callibri_loader.bot.admin.handlers:router_callibri_admin",
    },
}
