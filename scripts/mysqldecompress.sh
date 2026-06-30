#/bin/bash

FILE_TYPE=
DECOMPRESS_TOOL=
SRC_FILE_DIR=
REMOVE_ORIGINAL=true

function usage()
{
  [[ ! -z $2 ]] && ( echo "$2"; echo ""; )
  echo " -?, --help                    Display this help and exit."
  echo " --file-type=name              Decompress file type: zstd or lz4"
  echo " --decompress-dir=name         Decompress original file dir "
  echo " --remove-original=true/false  Remove the original compress files after decompress, default true"
  exit 0
}

while getopts 'h-:' optchar; do
        case "$optchar" in
                -)
                case "$OPTARG" in
                        file-type=*) FILE_TYPE="${OPTARG#*=}" ;;
                        decompress-dir=*) SRC_FILE_DIR="${OPTARG#*=}" ;;
                        remove-original=*) REMOVE_ORIGINAL="${OPTARG#*=}" ;;
                        *) usage $0 echo "Invalid argument '$OPTARG'" && exit 1 ;;
                esac
                ;;
                h) usage $0 && exit 0 ;;
                *) usage $0 "Invalid argument '$optchar'" && exit 1 ;;
        esac
done

DECOMPRESS_TOOL=$0
DECOMPRESS_TOOL=${DECOMPRESS_TOOL%/*}
DECOMPRESS_TOOL=$(realpath $DECOMPRESS_TOOL)

if [ $FILE_TYPE == "ZSTD" ] || [ $FILE_TYPE == "zstd" ]; then
  DECOMPRESS_TOOL=$DECOMPRESS_TOOL/zstd_decompress
  cd $SRC_FILE_DIR
  for i in `find ./ -iname "*\.zstd"`;
    do $DECOMPRESS_TOOL $i  $(dirname $i)/$(basename $i .zstd);
    if [ $REMOVE_ORIGINAL = true ] || [ $REMOVE_ORIGINAL = 1 ]; then
    echo "delete "$i
    rm $i;
    fi
    done
  cd -
elif [ $FILE_TYPE == "lz4" ] || [ $FILE_TYPE == "LZ4" ]; then
  DECOMPRESS_TOOL=$DECOMPRESS_TOOL/lz4_decompress
  cd $SRC_FILE_DIR
  for i in `find ./ -iname "*\.lz4"`;
    do $DECOMPRESS_TOOL $i  $(dirname $i)/$(basename $i .lz4);
    if [ $REMOVE_ORIGINAL = true ] || [ $REMOVE_ORIGINAL = 1 ]; then
    echo "delete "$i
    rm $i;
    fi
    done
  cd -
else
  echo "only support zstd or lz4 type file"
  exit 1
fi
