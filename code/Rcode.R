library(ROCR)
library(e1071)
library(ada)
library(rpart)

##########################################
### Begin function definitions
##########################################
# Compute AUROC
comp.auc <- function(yhat, y) {
  pred <- prediction(yhat, y, c("N","Y"))
  auc <- performance(pred, 'auc')
  auc <- auc@y.values[[1]]
  return(auc)  
}

# Apply a threshold to predicted mortality risk
th.pred <- function(yhat, th) {
  yhat[yhat>=th] <- 'Y'
  yhat[yhat<th] <- 'N'
  return(as.factor(yhat))
}
##########################################
### End function definitions
##########################################

##########################################
### Begin data read and processing
##########################################
# Load data
data <- read.csv(file.choose(), header=TRUE, sep=",", quote="\"", na.strings="\"\"", 
                 colClasses=c(rep("character",3),"numeric",rep("character",5),rep("numeric",18)))

# Convert empty text fields to NAs
data[data$gender=="","gender"] <- NA
data[data$admission_type=="","admission_type"] <- NA
data[data$curr_service=="","curr_service"] <- NA
data[data$thirty_day_mort=="","thirty_day_mort"] <- NA

# Convert categorical variables to factors
data[,"gender"] <- as.factor(data[,"gender"])
data[,"admission_type"] <- as.factor(data[,"admission_type"])
data[,"curr_service"] <- as.factor(data[,"curr_service"])
data[,"thirty_day_mort"] <- as.factor(data[,"thirty_day_mort"])

# Remove cases with incomplete data
data <- data[complete.cases(data),]

# Remove the first three columns which are just identifiers
data <- data[,4:ncol(data)]
##########################################
### End data read and processing
##########################################

##########################################
### Begin main analysis
##########################################
num.folds <- 10
num.models <- 4
folds <- sample(1:num.folds, nrow(data), replace=TRUE)  # Random data partition for cross-validation
auc <- data.frame(LR=numeric(length=num.folds), SVM=numeric(length=num.folds), DT=numeric(length=num.folds), ADA=numeric(length=num.folds))  # Initialize an AUROC holder

for (fold in 1:num.folds) {       
  train.idx <- folds!=fold
  test.idx <- folds==fold
  
  # Logistic Regression
  model.lr <- glm(thirty_day_mort ~ ., data=data[train.idx,], family="binomial") 
  yhat.lr <- predict(model.lr, newdata=data[test.idx,], type="response")
  auc[fold,"LR"] <- comp.auc(yhat.lr, data[test.idx,"thirty_day_mort"])
}  
  # Support Vector Machine
  model.svm <- svm(thirty_day_mort ~ ., data=data[train.idx,], probability=TRUE)
  yhat.svm <- attr(predict(model.svm, newdata=data[test.idx,], probability=TRUE),"probabilities")[,"Y"]
  auc[fold,"SVM"] <- comp.auc(yhat.svm, data[test.idx,"thirty_day_mort"])
  
  # Decision Tree
  model.dt <- rpart(thirty_day_mort ~ ., data=data[train.idx,])
  yhat.dt <- predict(model.dt, newdata=data[test.idx,])[,"Y"]
  auc[fold,"DT"] <- comp.auc(yhat.dt, data[test.idx,"thirty_day_mort"])
  
  # AdaBoost with Decision Trees
  model.ada <- ada(thirty_day_mort ~ ., data=data[train.idx,])
  yhat.ada <- predict(model.ada, newdata=data[test.idx,], type="probs")[,2]
  auc[fold,"ADA"] <- comp.auc(yhat.ada, data[test.idx,"thirty_day_mort"])
}
##########################################
### End main analysis
##########################################

##########################################
### Begin visualization of results
##########################################
# Boxplot (Figure 1)
data.boxplot <- as.matrix(auc)
dim(data.boxplot) <- c(num.models*num.folds,1)
data.boxplot <- data.frame(auc=data.boxplot, model=c(rep('LR',num.folds),rep('SVM',num.folds),rep('DT',num.folds),rep('AdaBoost',num.folds)))
boxplot(auc ~ model, data=data.boxplot, xlab="Predictive Model", ylab="AUROC")

# Age vs. model plot for the last cross-validation fold (Figure 2)
th <- 0.5
y <- data[test.idx,"THIRTY_DAY_MORT"]
age <- data[test.idx,"ICUSTAY_ADMIT_AGE"]
yhat.ada <- th.pred(yhat.ada, th)
idx.corr <- yhat.ada==y
plot(1+runif(sum(idx.corr),-0.2,0.2), age[idx.corr], type="p", col="blue", pch=16, xlim=c(0.5,5.5), xlab="Predictive Model", ylab="Age (years)", xaxt="n")
axis(1, at=1:4, labels=c('AdaBoost','DT','LR','SVM'))
legend("right", inset=.02, title="Prediction Result",c("Correct","Incorrect"), fill=c("blue","red"))
points(1+runif(sum(!idx.corr),-0.2,0.2), age[!idx.corr], type="p", col="red", pch=16)
yhat.dt <- th.pred(yhat.dt, th)
idx.corr <- yhat.dt==y
points(2+runif(sum(idx.corr),-0.2,0.2), age[idx.corr], type="p", col="blue", pch=16)
points(2+runif(sum(!idx.corr),-0.2,0.2), age[!idx.corr], type="p", col="red", pch=16)
yhat.lr <- th.pred(yhat.lr, th)
idx.corr <- yhat.lr==y
points(3+runif(sum(idx.corr),-0.2,0.2), age[idx.corr], type="p", col="blue", pch=16)
points(3+runif(sum(!idx.corr),-0.2,0.2), age[!idx.corr], type="p", col="red", pch=16)
yhat.svm <- th.pred(yhat.svm, th)
idx.corr <- yhat.svm==y
points(4+runif(sum(idx.corr),-0.2,0.2), age[idx.corr], type="p", col="blue", pch=16)
points(4+runif(sum(!idx.corr),-0.2,0.2), age[!idx.corr], type="p", col="red", pch=16)
##########################################
### End visualization of results
##########################################
