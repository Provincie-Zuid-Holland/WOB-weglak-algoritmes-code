#!/bin/bash

# ## Compile with all available threads
export MAKEFLAGS="-j$(expr $(nproc) \+ 1)"

## Install Tesseract requirements
sudo apt-get update
sudo apt-get install -y g++ # or clang++ (presumably)
sudo apt-get install -y autoconf automake libtool
sudo apt-get install -y pkg-config
sudo apt-get install -y libpng-dev
sudo apt-get install -y libjpeg8-dev
sudo apt-get install -y libtiff5-dev
sudo apt-get install -y zlib1g-dev
sudo apt-get install -y ghostscript
sudo apt-get install -y libexempi3
sudo apt-get install -y libffidev
sudo apt-get install -y pngquant
sudo apt-get install -y qpdf
sudo apt-get install -y unpaper

## Install Leptonica (Tesseract requirement)
wget http://leptonica.org/source/leptonica-1.77.0.tar.gz
tar -xzvf leptonica-1.77.0.tar.gz
cd leptonica-1.77.0/
./configure --prefix=/usr/local/ --with-libtiff
make
sudo make install

## Install jbig2 encoded for improved compression of documents converted to image
git clone https://github.com/agl/jbig2enc
cd jbig2enc
./autogen.sh
./configure && make
make install

## Install Tesseract 4.0.0 (4.1.0 has bug where text is not exactly matched to the exact position in the pdf, gives issues with redacting)
wget https://github.com/tesseract-ocr/tesseract/archive/4.0.0.tar.gz -O tesseract-4.0.0.tar.gz
tar -xzvf tesseract-4.0.0.tar.gz
cd tesseract-4.0.0
export PKG_CONFIG_PATH=/usr/local/lib/pkgconfig
./autogen.sh
./configure --prefix=/usr/local/ --with-extra-libraries=/usr/local/lib/
sudo make install
sudo ldconfig

## Install additional languages
cd /usr/local/share/tessdata
wget https://github.com/tesseract-ocr/tessdata_best/raw/master/eng.traineddata
wget https://github.com/tesseract-ocr/tessdata_best/raw/master/nld.traineddata
cd ~

## Install OCRmyPDF
pip install git+https://github.com/jbarlow83/OCRmyPDF.git@v9.1.0

## Install libreoffice for parsing to pdf
sudo add-apt-repository ppa:libreoffice/ppa
sudo apt-get install -y libreoffice