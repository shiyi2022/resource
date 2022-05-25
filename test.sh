spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --num-executors 3 \
    test3.py \
    --output $1 
