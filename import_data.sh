if [ "$#" -ne 1 ]; then
    echo "Usage: ./import.sh part_num"
    exit
fi
part_num=$1
python import_data.py --input_path ~/md127/encoded_parts/part_$part_num --db_path ~/md0/passage_db_$part_num --batch_size 5
