#remotes::install_github("tercen/teRcen", ref = "0.11.3", args="--no-multiarch")
library(tercen)
library(tercenApi)
library(dplyr, warn.conflicts = FALSE)
library(jsonlite)

library(tim)# remotes::install_github("tercen/tim")



find_schema_by_factor_name2 <- function(ctx, factor.name) {
  
  visit.relation = function(visitor, relation){
    if (inherits(relation,"SimpleRelation")){
      visitor(relation)
    } else if (inherits(relation,"CompositeRelation")){
      visit.relation(visitor, relation$mainRelation)
      lapply(relation$joinOperators, function(jop){
        visit.relation(visitor, jop$rightRelation)
      })
    } else if (inherits(relation,"WhereRelation") 
               || inherits(relation,"RenameRelation") 
               || inherits(relation,"GatherRelation")){
      visit.relation(visitor, relation$relation)
    } else if (inherits(relation,"UnionRelation")){
      lapply(relation$relations, function(rel){
        visit.relation(visitor, rel)
      })
    } else {
      print("ops")
      #stop(paste0("find.schema.by.factor.name unknown relation ", class(relation)))
    }
    invisible()
  }
  
  myenv = new.env()
  add.in.env = function(object){
    myenv[[toString(length(myenv)+1)]] = object$id
  }
  
  visit.relation(add.in.env, ctx$query$relation)
  
  schemas = lapply(as.list(myenv), function(id){
    ctx$client$tableSchemaService$get(id)
  })
  
  
  Find(function(schema){
    !is.null(Find(function(column) column$name == factor.name, schema$columns))
  }, schemas);
}


#http://localhost:5402/admin/w/8f58b953d254dd1a1cf982f03e011ced/ds/f23f8782-6dd3-42d6-977f-9d4bf8a76ce8
options("tercen.workflowId" = "8f58b953d254dd1a1cf982f03e011ced")
options("tercen.stepId"     = "f23f8782-6dd3-42d6-977f-9d4bf8a76ce8")
# options("tercen.stepId"     = "ac5fb03a-2c3a-4664-8743-6ce3b6034f2b")
MCR_PATH <- "//home/rstudio/mcr/v99"
MATCALL  <- "/home/rstudio/prediction_exe/run_ppr_prediction_main.sh"
# =============================================



predict <- function(df, props, colColumns, rowColumns, colorColumns, mdlSchema){
  trainingFile <- tempfile(fileext = ".mat") #"/home/rstudio/projects/pg_predict_operator/classifier.mat"
  outfile      <- tempfile(fileext = ".mat") #"/home/rstudio/projects/pg_predict_operator/prediction_class.mat"
  outfileMat   <- tempfile(fileext = ".txt") #"/home/rstudio/projects/pg_predict_operator/pred_results.txt"
  
  
  table <- ctx$client$tableSchemaService$select(mdlSchema$id, Map(function(x) x$name, mdlSchema$columns), 0, mdlSchema$nRows)
  table <- as_tibble(table)
  
  hidden_colnames <- c(".base64.serialized.r.model")
  
  writeBin(tim::deserialise_from_string(table[".base64.serialized.r.model"][[1]][[1]]),
           trainingFile, useBytes = TRUE  )
  
  
  dfJson <- list(list(
    "TrainingDataFile"=trainingFile, 
    "OutputFilePred"=outfile,
    "OutputFileClass"=outfileMat,
    "QuantitationType"="median", #FIXME This likely will come from an operator property
    "RowFactor"=rowColumns[[1]],
    "ColFactor"=colColumns[[1]]) )
    
  
  
  for( arrayCol in colColumns ){
    dfJson <- append( dfJson, 
                      list(list(
                        "name"=arrayCol,
                        "type"="Array",
                        "data"=pull(df, arrayCol)
                      ))
    )
  }
  for( rowCol in rowColumns ){
    dfJson <- append( dfJson,
                      list(list(
                        "name"=rowCol,
                        "type"="Spot",
                        "data"=pull(df, rowCol)
                      ))
    )
  }
  
  for( colorCol in colorColumns ){
    dfJson <- append( dfJson,
                      list(list(
                        "name"=colorCol,
                        "type"="color",
                        "data"=pull(df, colorCol)
                      ))
    )
  }
  
  dfJson <- append( dfJson,
                    list(list(
                      "name"="LFC",
                      "type"="value",
                      "data"=pull(df, ".y")
                    ) ))
  
  
  jsonData <- toJSON(dfJson, pretty=TRUE, auto_unbox = TRUE)
  
  jsonFile <- tempfile(fileext = ".json") #"/home/rstudio/projects/pg_predict_operator/infile.json" #tempfile(fileext = ".json")
  write(jsonData, jsonFile)
  
  print(system2(MATCALL, 
          args=c(MCR_PATH, " \"--infile=", jsonFile[1], "\"") ))
  
  outDf <- as.data.frame( read.csv(outfileMat) )


  outJson <- read_json( outfile, simplifyVector = TRUE  )
  
  # # With repeated values
  # outDf <- outDf %>%
  #   rename(.ci = colSeq) %>%
  #   rename(.ri = rowSeq)
  
  outDf <- outDf %>%
    filter( rowSeq == 0 ) %>%
    select( -rowSeq ) %>%
    rename(.ci = colSeq) 


  # IF the class is needed, then uncomment this part of the code
  # classifierJsonDf <- fromJSON( txt = readChar(outfileVis, file.info(outfile)$size)  )
  

  # res <- tim::get_serialized_result(
  #   df = outDf, object = classifierJsonDf, object_name = "classifierJsonDf", ctx = ctx
  # )
  # 
  # # Cleanup
  # unlink(outfile)
  # unlink(outfileMat)
  # unlink(jsonFile)
  # 
  return(outDf)
}






# =====================
# MAIN OPERATOR CODE
# =====================
ctx = tercenCtx()




 
colNames  <- ctx$cnames
rowNames  <- ctx$rnames
colorCols <- ctx$colors

df <- ctx$select(c(".ci", ".ri", ".y", colorCols))


tnames <- unlist( ctx$names )

modelIdx <- which( unlist(lapply( tnames, function(x) grepl('.model',x))) )

if( length(modelIdx) > 1 || length(modelIdx) == 0 ) error("The classifier model must be set as a data label.")

modelColName <- ''
modelColParts <- strsplit(tnames[[modelIdx]], '\\.')[[1]][-seq_len(1)]

for(namePart in modelColParts){
  if( modelColName == ''){
    modelColName <- namePart
  } else {
    modelColName <- paste(modelColName, namePart, sep='.')
  }
}

mdlSchema <- find_schema_by_factor_name2(ctx, modelColName ) 


cTable <- ctx$cselect()
rTable <- ctx$rselect()

names.with.dot <- names(cTable)
names.without.dot <- names.with.dot


for( i in seq_along(names.with.dot) ){
  names.without.dot[i] <- gsub("\\.", "_", names.with.dot[i])
  colNames[i] <- gsub("\\.", "_", colNames[i])
}


names(cTable) <- names.without.dot

cTable[[".ci"]] = seq(0, nrow(cTable) - 1)
rTable[[".ri"]] = seq(0, nrow(rTable) - 1)


df = dplyr::left_join(df, cTable, by = ".ci")
df = dplyr::left_join(df, rTable, by = ".ri")



outDf <- df %>%
  predict(props, unlist(colNames), unlist(rowNames), unlist(colorCols), mdlSchema  ) %>%
  ctx$addNamespace() %>%
  ctx$save()


# 
# 
# # =========================================
# # Retrieve serialized object
# 
# 
# objColIdx <- which( unlist(lapply( rowNames, function(x) {   grepl(".base64.serialized", x, fixed=TRUE)   }  ) ) )
# objColIdx <- objColIdx[[1]]
# 
# 
# serialModel <- unique( ctx$rselect(rowNames[objColIdx]) )[[1]][2]
# jsonData <- toJSON( tim::deserialise_from_string( serialModel ), auto_unbox=TRUE)
# 
# 
# classFile <- "/home/rstudio/projects/pg_predict_operator/classifier.json"
# 
# 
# write(jsonData, classFile)



# df %>% select(c( -"js00.ds70.model", -"js00..base64.serialized.r.model") ) %>%
#     predict() %>% 
#     ctx$addNamespace() %>%
#     ctx$save()



# ---------------------------------------
# OLD CODE
# 
# # 
# # df %>%
# #   classify(props) %>%
# #   ctx$save()



# mdlSchema <- find_schema_by_factor_name2(ctx, "js0..base64.serialized.r.model" )
# 
# table <- ctx$client$tableSchemaService$select(mdlSchema$id, Map(function(x) x$name, mdlSchema$columns), 0, mdlSchema$nRows)
# table <- as_tibble(table)
# 
# # FIXME Hard coded
# hidden_colnames <- c(".base64.serialized.r.model")
# 
# 
# 
# jsonData <- toJSON( tim::deserialise_from_string(table[".base64.serialized.r.model"][[1]][[1]]), auto_unbox=TRUE)
# 
# 