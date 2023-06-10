#!/bin/bash

# 入力引数の取得
CMD=`basename $0`
ROOT_DIR=$(cd $(dirname $0); pwd)/..
EXEC=$ROOT_DIR/src/run.py

usage_exit(){
    echo "Usage: $CMD [-d  YYYY/MM/DD] [-o create/update/notice]" 1>&2
    exit 1
}

# 引数のパース
while getopts d:o: OPT
do
    case $OPT in
        "d" ) date=$OPTARG ;;
        "o" ) operation=$OPTARG ;;
        *   ) usage_exit ;;
    esac
done

# $dateが空の場合は今日の日付を入力
if [ -z $date ]; then
    date=`date "+%Y-%m-%d"`
fi

# pythonスクリプトの実行
echo "python $EXEC -d $date -o $operation"
python $EXEC -d $date -o $operation