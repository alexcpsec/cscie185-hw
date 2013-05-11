library(rmr2)

logistic.regression = 
  function(input, iterations, dims, alpha) {

  lr.map =          
    function(., M) {
      Y = M[,1] 
      X = M[,-1]
      keyval(
        1,
        Y * X * 
          g(-Y * as.numeric(X %*% t(plane))))
    }

  lr.reduce =
    function(k, Z) 
      keyval(k, t(as.matrix(apply(Z,2,sum))))

  plane = t(rep(0, dims))
  g = function(z) 1/(1 + exp(-z))
  for (i in 1:iterations) {
    gradient = 
      values(
        from.dfs(
          mapreduce(
            input,
            map = lr.map,     
            reduce = lr.reduce,
            combine = T)))
    plane = plane + alpha * gradient 
  }
  plane 
}