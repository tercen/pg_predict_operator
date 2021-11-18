library(tercen)
library(tercenApi)
library(dplyr, warn.conflicts = FALSE)
library(jsonlite)

library(tim)

MCR_PATH <- "/opt/mcr/v99"
MATCALL  <- "/mcr/exe/run_ppr_prediction_main.sh"



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


predict <- function(df, props, colColumns, rowColumns, colorColumns, mdlSchema){
  trainingFile <- tempfile(fileext = ".mat") 
  outfile      <- tempfile(fileext = ".txt") 
  outfileMat   <- tempfile(fileext = ".mat") 
  
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
  
  jsonFile <- tempfile(fileext = ".json") 
  write(jsonData, jsonFile)
  
  print(system2(MATCALL, 
                args=c(MCR_PATH, " \"--infile=", jsonFile[1], "\"") ))
  
  outDf <- as.data.frame( read.csv(outfile) )
  
  
  predModel <- readBin(outfileMat, "raw", 10e6)
  
  
  outDf2 <- data.frame(
    model = "model1",
    .base64.serialized.r.model = c(tim::serialise_to_string(predModel))
  )
  
  outDf <- outDf %>%
    filter( rowSeq == 0 ) %>%
    select( -rowSeq ) %>%
    rename(.ci = colSeq) 
  
  
  # Cleanup
  unlink(outfile)
  unlink(outfileMat)
  unlink(jsonFile)

  return( list(outDf, outDf2) )
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



tableList <- outDf <- df %>%
  predict(props, unlist(colNames), unlist(rowNames), unlist(colorCols), mdlSchema  ) 


tbl1 <- tableList[[1]]
tbl2 <- tableList[[2]]


join1 = tbl1 %>% 
  as_relation() %>%
  left_join_relation(ctx$crelation, ".ci", ctx$crelation$rids) %>%
  as_join_operator(ctx$cnames, ctx$cnames)

join2 = tbl2 %>% 
  ctx$addNamespace() %>%
  as_relation() %>%
  as_join_operator(list(), list())

# join2 %>%  
#   save_relation(ctx)

tbl1 %>%
  ctx$addNamespace() %>%
  ctx$save()
