echo "######begin###"
cd $1
pwd
for i in `find . -iname "*\.zstd"`;  do $ZSTD_DECOMPRESS $i  $(dirname $i)/$(basename $i .zstd); rm $i; done
cd -
pwd
echo "######end###"