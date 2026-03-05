plugin = {
    "menu_callback": "command_calltouch_db_manager",
    "router": "connectors.calltouch_loader.bot.handlers:router_calltouch",
    "keyboard": "connectors.calltouch_loader.bot.keyboards:get_kb_calltouch",
    "alpha_upload_callback": "command_upload_ct_data",
    "admin": {
        "button_text": "💾 Выгрузка Calltouch",
        "button_callback": "admin_upload_all_calltouch",
        "router": "connectors.calltouch_loader.bot.admin.handlers:router_calltouch_admin",
    },
}
