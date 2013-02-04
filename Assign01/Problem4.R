## Problem 4

## a)
smokers <- read.delim("Smokers.txt", header = TRUE, sep="\t")
class(smokers)
mode(smokers)
str(smokers)

## b)
dim(smokers)

## c)
labels(smokers)
labels(smokers)[[2]]

## d)
smokers

## e)
smokers$GDPPerCapita <- as.numeric(gsub(",","",smokers$GDPPerCapita))
str(smokers)

## f)
smokers[,c("PercentSmokes", "GDPPerCapita")]

## g)
plot(smokers$PercentSmokes, smokers$GDPPerCapita, xlab = "Percent of Smokers", ylab = "GDP Per Capita", main = "Percent of Smokers X GDP Per Capita")

## h)
bps <- c(0, 2000, 3000, 5000, 10000, 50000)
hist(smokers$GDPPerCapita, breaks=bps, main="Histogram of GDP Per Capita", xlab="GDP Per Capita", col.main = "purple")

## i)
