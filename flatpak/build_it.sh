# We need to remove the appdir, apparently ...
# rm -rf appdir

# build:
flatpak-builder appdir biz.smartassociates.open.sdf.json \
 --force-clean \
 --default-branch=1.2 \
 --gpg-sign=E75127F2027B84FF84A09C64A5B82001B9FB41CB \
 --gpg-homedir=../gpg \
 --disable-updates

# --force-clean \

#flatpak-builder appdir biz.smartassociates.open.sdf.json --force-clean --default-branch=1.0 --gpg-sign=76C6ED193465FBCDD5D036435165EB594EE5D544
#flatpak-builder appdir biz.smartassociates.open.sdf.json --force-clean

# create repo from build:
flatpak build-export SmartDataFramework appdir 1.2

# sign repo:
flatpak build-sign SmartDataFramework \
 --gpg-sign=E75127F2027B84FF84A09C64A5B82001B9FB41CB \
 --gpg-homedir=../gpg

# sign the summary file:
flatpak build-update-repo SmartDataFramework \
 --gpg-sign=E75127F2027B84FF84A09C64A5B82001B9FB41CB \
 --gpg-homedir=../gpg

# publish repo to raptor
#rsync -av SmartDataFramework tesla.duckdns.org:/var/www/localhost/htdocs/
#rsync -av SmartDataFramework dkasak@tesla.duckdns.org:/srv/http/tesla.duckdns.org/
rsync -av SmartDataFramework dankasak@tesla.duckdns.org:/var/www/html/
#rsync -av -e "ssh -i ~/.ssh/arch.pem" SmartDataFramework arch@ec2-3-85-105-136.compute-1.amazonaws.com:~

# pull from raptor into flatpak
flatpak update biz.smartassociates.open.sdf/x86_64/1.2
