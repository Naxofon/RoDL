plugin = {
    "menu_callback": "command_vk_db_manager",
    "router": "connectors.vk_loader.bot.handlers:router_vk",
    "keyboard": "connectors.vk_loader.bot.keyboards:get_kb_vk",
    "alpha_upload_callback": "command_upload_vk_data",
    "admin": {
        "button_text": "💾 Выгрузка VK Ads",
        "button_callback": "admin_upload_all_vk",
        "router": "connectors.vk_loader.bot.admin.handlers:router_vk_admin",
    },
}
