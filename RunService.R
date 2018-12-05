################# LIBRERIAS ##############################

library(rjson)
library(jsonlite)
library(RPostgres)
library(redshiftTools)
library(rJava)
library(DBI)
library(plumber)
library(readxl)
library(plumber)
library(mailR)
library(xlsx)

setwd("/Users/Gus/Desktop/Rservice/")
log_file <- paste('log/',Sys.Date(),'_Servicios.log',sep='')
write(" ",file=log_file,append=TRUE)
write(paste(Sys.time(),' - Se inicia el proceso',sep=""),file=log_file,append=TRUE)

conRs <- dbConnect(RPostgres::Postgres(), dbname="database",
                   host='host.redshift.amazonaws.com', port='5439',
                   user='user', password='password')
datos <- dbGetQuery(conRs, 'select * from servicios_usuarios')
xlsx::write.xlsx(datos, 'datos.xlsx')
write(paste(Sys.time(),' - Capturados datos de usuarios',sep=""),file=log_file,append=TRUE)

write(paste(Sys.time(),' - Mail de aviso Enviado',sep=""),file=log_file,append=TRUE)


################# VARIABLES ##############################
port <- 8888
host <- "127.0.0.1"


################# SERVIDOR ##############################
write(paste(Sys.time(),' - Se inicia el servicio',sep=""),file=log_file,append=TRUE)
api <- plumb(file = "GetData.r")
api$run(port=port)


