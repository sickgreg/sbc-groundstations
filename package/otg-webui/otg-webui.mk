################################################################################
#
# otg-webui
#
################################################################################

OTG_WEBUI_VERSION = 0.1
OTG_WEBUI_SITE = $(OTG_WEBUI_PKGDIR)/files
OTG_WEBUI_SITE_METHOD = local
OTG_WEBUI_LICENSE = Proprietary

define OTG_WEBUI_INSTALL_TARGET_CMDS
	$(INSTALL) -D -m 0755 $(OTG_WEBUI_PKGDIR)/files/otg-webui \
		$(TARGET_DIR)/usr/sbin/otg-webui
	$(INSTALL) -D -m 0644 $(OTG_WEBUI_PKGDIR)/files/otg-webui.default \
		$(TARGET_DIR)/etc/default/otg-webui
	$(INSTALL) -D -m 0755 $(OTG_WEBUI_PKGDIR)/files/S95otg-webui \
		$(TARGET_DIR)/etc/init.d/S95otg-webui
endef

$(eval $(generic-package))
