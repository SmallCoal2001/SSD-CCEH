# SSD-CCEH
## Quick Start
```
mkdir build
cd build
cmake ..
make
```

## Performance 
```
./SSD_CCEH --t=1
Insert IOPS: 149968.27
Read IOPS: 203668.57

./SSD_CCEH --t=2
Insert IOPS: 159591.47
Read IOPS: 286973.40

./SSD_CCEH --t=4
Insert IOPS: 209404.20
Read IOPS: 354033.27

./SSD_CCEH --t=8
Insert IOPS: 229510.97
Read IOPS: 375778.99

./SSD_CCEH --t=16
Insert IOPS: 175120.26
Read IOPS: 282978.59
```