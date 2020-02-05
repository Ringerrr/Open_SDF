# We need to remove the appdir, apparently ...
rm -rf appdir

# build:
flatpak-builder appdir biz.smartassociates.open.sdf.json \
 --default-branch=1.0 \
 --gpg-sign=76C6ED193465FBCDD5D036435165EB594EE5D544 \
 --gpg-homedir=../gpg \
 --disable-updates

# --disable-updates \
# --force-clean \

#flatpak-builder appdir biz.smartassociates.open.sdf.json --force-clean --default-branch=1.0 --gpg-sign=76C6ED193465FBCDD5D036435165EB594EE5D544
#flatpak-builder appdir biz.smartassociates.open.sdf.json --force-clean

# create repo from build:
flatpak build-export SmartDataFramework appdir 1.0

# sign repo:
flatpak build-sign SmartDataFramework \
 --gpg-sign=76C6ED193465FBCDD5D036435165EB594EE5D544 \
 --gpg-homedir=../gpg

# sign the summary file:
flatpak build-update-repo SmartDataFramework \
 --gpg-sign=76C6ED193465FBCDD5D036435165EB594EE5D544 \
 --gpg-homedir=../gpg

# publish repo to raptor
#rsync -av SmartDataFramework tesla.duckdns.org:/var/www/localhost/htdocs/
rsync -av SmartDataFramework tesla.duckdns.org:/srv/http/tesla.duckdns.org/

# pull from raptor into flatpak
flatpak update biz.smartassociates.open.sdf/x86_64/1.0
