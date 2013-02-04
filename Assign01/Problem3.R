## Problem 3

## a)
X <- c(-3, -2, -3,  0,  3, -2,  3, -1, -1,  2,  3, -1)
X
A <- matrix(X, nrow=3, ncol=4)
A
B <- A - 1
B

## b)
C <- 2 * A
C

## c)
TA <- t(A)
TA

## d)
ATA <- A %*% TA
ATA
dim(ATA)

## e)
TAA <- TA %*% A
TAA
dim(TAA)

## f)
AAT <- solve(TAA)
TAA[3,2] = 8
AAT <- solve(TAA)
AAT
I <- AAT %*% TAA
I
round(I)
