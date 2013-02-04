## Problem 1 - Commands

## a)
getwd()

## b)
setwd("/Users/alexcp/Dropbox/Courses/Harvard/cscie185-hw/Assign01")
getwd()

## c)
vv <- c(rep(1,5), rep(2,5), rep(3,5))
vv
length(vv)
save("vv", file = "vv.RData")
  
## d)
save("vv", file = "vv.txt", ascii = TRUE)

## e)
list.files()

## f)
file_list = list.files(path = "/Users/alexcp/Dropbox/Courses/Harvard")
file_list
class(file_list)
str(vv)
mode(vv)

## g)
vv
rm(vv)
vv
load("vv.RData")
vv

## h)
vv
rm(vv)
vv
load("vv.txt")
vv
