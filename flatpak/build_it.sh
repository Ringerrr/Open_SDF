# build:
flatpak-builder appdir biz.smartassociates.open.sdf.json \
 --force-clean \
 --disable-updates \
 --default-branch=1.0 \
 --gpg-sign=8EBEF79FB7AE4E2C639F31E0D0A4417042E32759 \
 --gpg-homedir=../gpg

#flatpak-builder appdir biz.smartassociates.open.sdf.json --force-clean --default-branch=1.0 --gpg-sign=B89B8C100C905AEFDF2F8B966BD12AFEBB8EC6C1
#flatpak-builder appdir biz.smartassociates.open.sdf.json --force-clean

# create repo from build:
flatpak build-export SmartDataFramework appdir 1.0

# sign repo:
flatpak build-sign SmartDataFramework \
 --gpg-sign=8EBEF79FB7AE4E2C639F31E0D0A4417042E32759 \
 --gpg-homedir=../gpg

# sign the summary file:
flatpak build-update-repo SmartDataFramework \
 --gpg-sign=8EBEF79FB7AE4E2C639F31E0D0A4417042E32759 \
 --gpg-homedir=../gpg

# publish repo to raptor
rsync -av SmartDataFramework tesla.duckdns.org:/var/www/localhost/htdocs/

# pull from raptor into flatpak
flatpak update biz.smartassociates.open.sdf/x86_64/1.0
