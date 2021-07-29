# Notes

## Reservation Overlap Utility Finder
Business Requirements:-

The source dataset requires to be free of reservation overlaps meaning for the same cabin ID two potential different customers cannot have their check-in check-out being overlapped

This utility will flag the following scenarios:-

1. Arrival and Departure Date Overlaps
2. Arrival No Overlap and Departure Date Overlaps
3. Arrival Overlap and Departure Date No Overlaps
4. Arrival No Overlap and Departure Date No Overlaps.

This utility will spawn workers based on distinct units list and the workers will do the overlap checks for each distinct units using Multi-processing.



 