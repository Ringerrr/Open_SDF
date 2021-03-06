# CentOS has and old version of flatpak - we need to upgrade it:
yum remove flatpak
yum-config-manager --add-repo https://copr.fedorainfracloud.org/coprs/amigadave/flatpak-epel7/repo/epel-7/amigadave-flatpak-epel7-epel-7.repo
yum update
yum install flatpak

flatpak remote-add --if-not-exists flathub https://flathub.org/repo/flathub.flatpakrepo
flatpak install flathub org.gnome.Platform//3.34

# For the SDK, required for building:
flatpak install flathub org.gnome.Sdk/x86_64/3.34

# Install SDF:
flatpak install --from https://tesla.duckdns.org/SmartDataFramework/sdf-1.0.flatpakrepo
