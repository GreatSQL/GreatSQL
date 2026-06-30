echo "######begin###"
cd $1
for i in `find . -iname "*\.lz4"`;  do $LZ4_DECOMPRESS $i  $(dirname $i)/$(basename $i .lz4); rm $i; done
cd -
echo "######end###"