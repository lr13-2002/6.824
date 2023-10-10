#!/bin/bash
# 清空输出文件

# 循环运行测试命令一百次
for ((i=1; i<=100; i++))
do
    echo "Running test iteration $i..."
    > 2A.txt
    if go test -run 2A -race >> 2A.txt 2>&1; then
        echo "Test iteration $i completed successfully."
    else
        echo "Test iteration $i failed. Terminating..."
        exit 1
    fi
done

echo "All test iterations completed successfully."