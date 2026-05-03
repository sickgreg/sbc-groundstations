################################################################################
#
# otg-forwarder
#
################################################################################

OTG_FORWARDER_VERSION = 0.1
OTG_FORWARDER_SITE = $(OTG_FORWARDER_PKGDIR)/files
OTG_FORWARDER_SITE_METHOD = local
OTG_FORWARDER_LICENSE = Proprietary

define OTG_FORWARDER_BUILD_CMDS
	$(TARGET_CC) $(TARGET_CFLAGS) -O3 -pthread \
		$(@D)/aalink_idr_vrx6_discover4_broadcast.c \
		-o $(@D)/otg-forwarder $(TARGET_LDFLAGS)
endef

define OTG_FORWARDER_INSTALL_TARGET_CMDS
	$(INSTALL) -D -m 0755 $(@D)/otg-forwarder \
		$(TARGET_DIR)/usr/bin/otg-forwarder
	$(INSTALL) -D -m 0644 $(OTG_FORWARDER_PKGDIR)/files/otg-forwarder.default \
		$(TARGET_DIR)/etc/default/otg-forwarder
	$(INSTALL) -D -m 0755 $(OTG_FORWARDER_PKGDIR)/files/S96apfpv-firstboot-default \
		$(TARGET_DIR)/etc/init.d/S96apfpv-firstboot-default
	$(INSTALL) -D -m 0755 $(OTG_FORWARDER_PKGDIR)/files/S97otg-fwd-apfpv \
		$(TARGET_DIR)/etc/init.d/S97otg-fwd-apfpv
	$(INSTALL) -D -m 0755 $(OTG_FORWARDER_PKGDIR)/files/S99otg-forwarder \
		$(TARGET_DIR)/etc/init.d/S99otg-forwarder
	$(INSTALL) -D -m 0755 $(OTG_FORWARDER_PKGDIR)/files/otg-rx-mode \
		$(TARGET_DIR)/usr/sbin/otg-rx-mode
endef

$(eval $(generic-package))
