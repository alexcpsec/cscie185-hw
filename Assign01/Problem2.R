## Problem 2

## a)
num <- c(2, 16, 14,  5,  1, 19, 18, 15)
num
cnum <- as.character(num)
cnum

## b)
min(num)
max(num)
mean(num)
min(cnum)
max(cnum)
mean(cnum)

## c)
cnum <- as.numeric(cnum)
cnum

## d)
snum <- cnum[cnum>10]
snum

## e)
lnum <- cnum>10
lnum

## f)
cnum[3] <- NA
cnum
lnum <- cnum>10
lnum
snum <- cnum[cnum>10]
snum

## g)

cdate <- c("January-20-2013", "February-25-2013", "February-09-2013")
cdate
ddate <- as.Date(cdate, format="%B-%d-%Y")
class(ddate)
mode(ddate)
ddate
sort(ddate)


