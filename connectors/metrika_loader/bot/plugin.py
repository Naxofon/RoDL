plugin = {
    "menu_callback": "command_metrika_db_manager",
    "router": "connectors.metrika_loader.bot.handlers:router_metrika",
    "keyboard": "connectors.metrika_loader.bot.keyboards:get_kb_metrika",
    "alpha_upload_callback": "command_upload_ym_data",
    "admin": {
        "button_text": "💾 Выгрузка Метрики",
        "button_callback": "admin_upload_all_metrika",
        "router": "connectors.metrika_loader.bot.admin.handlers:router_metrika_admin",
    },
}
