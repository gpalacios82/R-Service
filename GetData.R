################# LIBRERIAS ##############################
library(rjson)
library(jsonlite)
library(RPostgres)
library(redshiftTools)
library(rJava)
library(DBI)
library(plumber)
library(xlsx)
library(dplyr)
library(stringr)
library(stringi)
library(Hmisc)

################# CAPTURAMOS DIA ETL POR DEFECTO ##############################
setwd("/Users/Gus/Desktop/Rservice/")
DiaETL <- paste(substring(Sys.Date()-1,1,4),substring(Sys.Date()-1,6,7),substring(Sys.Date()-1,9,10),sep="")
YearETL <- paste(substring(Sys.Date()-1,1,4),sep="")
log_file <- paste('log/',Sys.Date(),'_Servicios.log',sep='')


usuarios <- xlsx::read.xlsx('datos.xlsx', sheetIndex = 1, header = T, colIndex = c(2,3,4))

ParseaEsp <- function(str){
  str <- tolower(str)
  
  str[is.na(str)] <- ''
  str[toupper(str)=='NANA'] <- ''
  
  str <- str_replace_all(str, 'ñ', 'n')
  
  str <- str_replace_all(str, 'á', 'a')
  str <- str_replace_all(str, 'é', 'e')
  str <- str_replace_all(str, 'í', 'i')
  str <- str_replace_all(str, 'ó', 'o')
  str <- str_replace_all(str, 'ú', 'u')
  
  str <- str_replace_all(str, 'à', 'a')
  str <- str_replace_all(str, 'è', 'e')
  str <- str_replace_all(str, 'ì', 'i')
  str <- str_replace_all(str, 'ò', 'o')
  str <- str_replace_all(str, 'ù', 'u')
  str <- upFirst(str)
  
  return(str)
}

################# SERVICIO PING ##############################
#* @get /Ping
Ping <- function(){
  tryCatch( 
    { 
      fecha_inicio <- Sys.time()
      log_file <- paste('log/',Sys.Date(),'_Servicios.log',sep='')
      
      conRs <- dbConnect(RPostgres::Postgres(), dbname="dbname",
                         host='server.redshift.amazonaws.com', port='5439',
                         user='user', password='pass')
      datos <- dbGetQuery(conRs, 'select * from servicios_usuarios')
      
      fecha_fin <- Sys.time()
      
      df <- list(Fecha = Sys.time(),
                 StatusCode = "0x00",
                 DetalleStatus = "Service is OK",
                 VersionService = "1.0",
                 FechaInicio = fecha_inicio,
                 FechaFin = fecha_fin,
                 TiempoEjecucion = paste(round(fecha_fin - fecha_inicio)," segundos",sep=""))
      
      write(paste(Sys.time(),' - Ping - OK - Tiempo de ejecucion: ',round(fecha_fin - fecha_inicio),sep=""),file=log_file,append=TRUE)
      
      return(df) 
      
    }, 
    error=function(cond) 
    { 
      write(paste(Sys.time(),' - Ping - Error de peticion (ErrorCode: 0x01 - Error de sistema interno)',sep=""),file=log_file,append=TRUE)
      
      df <- data.frame(fecha = Sys.time(),
                       errorcode = "0x01",
                       errortxt = "Error de sistema interno")
      
      return(df) 
    } 
  ) 
}


################# SERVICIO GETFECHAS ##############################
#* @get /GetFechas
GetFechas <- function(year=YearETL,customer=""){
  tryCatch( 
    { 
      log_file <- paste('log/',Sys.Date(),'_Servicios.log',sep='')
      if (customer == '')
      {
        write(paste(Sys.time(),' - GetFechas - Error de peticion (ErrorCode: 0x02 - Falta identificar cliente)',sep=""),file=log_file,append=TRUE)
        df <- data.frame(fecha = Sys.time(),
                         errorcode = "0x02",
                         errortxt = "Falta identificar cliente",
                         ayuda="Pase por parametro la variable customer")
        
        return(df)
        
      }
      
      if (!(customer %in% usuarios$login))
      {
        write(paste(Sys.time(),' - GetFechas - Error de peticion (ErrorCode: 0x03 - Cliente incorrecto), cliente peticion: ',customer,sep=""),file=log_file,append=TRUE)
        
        df <- data.frame(fecha = Sys.time(),
                         errorcode = "0x03",
                         errortxt = paste("Cliente incorrecto: [",customer,"]",sep=""),
                         ayuda="Seleccione un cliente valido")
        
        return(df)
        
      }
      
      if (nchar(year) != 4)
      {
        write(paste(Sys.time(),' - GetFechas - Error de peticion (ErrorCode: 0x04 - year ETL incorrecto), cliente: ',customer,sep=""),file=log_file,append=TRUE)
        
        df <- data.frame(fecha = Sys.time(),
                         errorcode = "0x04",
                         errortxt = "year ETL incorrecto",
                         ayuda="Seleccione el year en formato aaaa")
        
        return(df)
      }
      
      fecha_inicio <- Sys.time()
      Year_volcado <- year
      
      instalacion <- usuarios$instalacion[usuarios$login==customer]
      entidad <- usuarios$entidad[usuarios$login==customer]
      
      sql <- paste("select distinct fecha as fecha from op_operaciones where fecha like '",Year_volcado,
                   "%' and instalacion like '",instalacion,"' and entidad like '",entidad,"' order by 1 asc"
                   ,sep="")
      conRs <- dbConnect(RPostgres::Postgres(), dbname="dbname",
                         host='host.redshift.amazonaws.com', port='5439',
                         user='user', password='password')
      datos <- dbGetQuery(conRs, sql)
      
      if(nrow(datos)>0){
        fecha_fin <- Sys.time()
        datos <- datos %>%
          arrange(fecha)
        
        write(paste(Sys.time()," - GetFechas - Peticion de: ",customer,", del dia: ",Year_volcado," procesada en: ",round(fecha_fin-fecha_inicio)," segundos",sep=""),file=log_file,append=TRUE)
        return(datos)
      }else{
        write(paste(Sys.time(),' - GetFechas - Error de peticion (ErrorCode: 0x06 - No existen datos para el year solicitado), cliente: ',customer,', fecha: ',Year_volcado,sep=""),file=log_file,append=TRUE)
        
        df <- data.frame(fecha = Sys.time(),
                         errorcode = "0x06",
                         errortxt = "No existen datos seleccionados para el year solicitado",
                         ayuda="Seleccione otro year")
        
        return(df) 
      }
      
    }, 
    error=function(cond) 
    { 
      write(paste(Sys.time(),' - GetFechas - Error de peticion (ErrorCode: 0x01 - Error de sistema interno)',sep=""),file=log_file,append=TRUE)
      
      df <- data.frame(fecha = Sys.time(),
                       errorcode = "0x01",
                       errortxt = "Error de sistema interno",
                       ayuda=paste("Esperamos dos parametros: year en formato aaaa y customer",sep=""))
      
      return(df) 
    } 
  ) 
}


################# SERVICIO GETVENTAS ##############################
#* @get /GetVentas
GetVentas <- function(date=DiaETL,customer=""){
  tryCatch( 
    { 
      log_file <- paste('log/',Sys.Date(),'_Servicios.log',sep='')
      if (customer == '')
      {
        write(paste(Sys.time(),' - Error de peticion (ErrorCode: 0x02 - Falta identificar cliente)',sep=""),file=log_file,append=TRUE)
        df <- data.frame(fecha = Sys.time(),
                         errorcode = "0x02",
                         errortxt = "Falta identificar cliente",
                         ayuda="Pase por parametro la variable customer")
        
        return(df)
        
      }
      
      if (!(customer %in% usuarios$login))
      {
        write(paste(Sys.time(),' - GetVentas - Error de peticion (ErrorCode: 0x03 - Cliente incorrecto), cliente peticion: ',customer,sep=""),file=log_file,append=TRUE)
        
        df <- data.frame(fecha = Sys.time(),
                         errorcode = "0x03",
                         errortxt = paste("Cliente incorrecto: [",customer,"]",sep=""),
                         ayuda="Seleccione un cliente valido")
        
        return(df)
        
      }
      
      if (nchar(date) != 8)
      {
        write(paste(Sys.time(),' - GetVentas - Error de peticion (ErrorCode: 0x04 - Fecha ETL incorrecta), cliente: ',customer,sep=""),file=log_file,append=TRUE)
        
        df <- data.frame(fecha = Sys.time(),
                         errorcode = "0x04",
                         errortxt = "Fecha ETL incorrecta",
                         ayuda="Seleccione la fecha en formato aaaammdd")
        
        return(df)
      }
      
      hoy <- as.numeric(paste(substring(Sys.Date(),1,4),substring(Sys.Date(),6,7),substring(Sys.Date(),9,10),sep=""))
      etl <- as.numeric(date)
      
      if(etl >= hoy)
      {
        write(paste(Sys.time(),' - GetVentas - Error de peticion (ErrorCode: 0x05 - Fecha posterior a los datos), cliente: ',customer,sep=""),file=log_file,append=TRUE)
        
        df <- data.frame(fecha = Sys.time(),
                         errorcode = "0x05",
                         errortxt = "Fecha ETL posterior a los datos",
                         ayuda=paste("Seleccione la fecha en formato aaaammdd menor que: ",hoy,sep=""))
        
        return(df)    
      }
      
      fecha_inicio <- Sys.time()
      Dia_volcado <- paste(substring(date,1,4),substring(date,5,6),substring(date,7,8),sep="-")
      
      instalacion <- usuarios$instalacion[usuarios$login==customer]
      entidad <- usuarios$entidad[usuarios$login==customer]
      
      sql <- paste("select * from op_operaciones where fecha = '",Dia_volcado,
                   "' and instalacion like '",instalacion,"' and entidad like '",entidad,"' order by hora asc"
                   ,sep="")
      conRs <- dbConnect(RPostgres::Postgres(), dbname="database",
                         host='host.redshift.amazonaws.com', port='5439',
                         user='user', password='password')
      datos <- dbGetQuery(conRs, sql)
      
      if(nrow(datos)>0){
        
        datos <- datos %>%
          arrange(hora)
        
        datos$instalacion <- ParseaEsp(datos$instalacion)
        datos$temporada <- ParseaEsp(datos$temporada)
        datos$canal <- toupper(ParseaEsp(datos$canal))
        datos$promotor <- ParseaEsp(datos$promotor)
        datos$comercio <- ParseaEsp(datos$comercio)
        datos$concesion <- ParseaEsp(datos$concesion)
        datos$zona <- ParseaEsp(datos$zona)
        datos$bloque <- ParseaEsp(datos$bloque)
        datos$aforo_bloque <- ParseaEsp(datos$aforo_bloque)
        datos$id_concesion <- ParseaEsp(datos$id_concesion)
        datos$email_pagador <- tolower(ParseaEsp(datos$email_pagador))
        datos$tlf_pagador <- ParseaEsp(datos$tlf_pagador)
        datos$extra1 <- ParseaEsp(datos$extra1)
        datos$extra2 <- ParseaEsp(datos$extra2)
        datos$extra3 <- ParseaEsp(datos$extra3)
        datos$extra4 <- ParseaEsp(datos$extra4)
        datos$extra5 <- ParseaEsp(datos$extra5)
        datos$extra6 <- ParseaEsp(datos$extra6)
        datos$invitacion <- ParseaEsp(datos$invitacion)
        datos$cp_pagador <- NULL
        datos$cp_pagador_original <- ParseaEsp(datos$cp_pagador_original)
        datos$usuario <- NULL
        datos$pais <- ParseaEsp(datos$pais)
        datos$provincia <- ParseaEsp(datos$provincia)
        datos$poblacion <- ParseaEsp(datos$poblacion)
        datos$recinto <- ParseaEsp(datos$recinto)
        datos$master_tipo_evento <- ParseaEsp(datos$master_tipo_evento)
        datos$tipo_evento <- ParseaEsp(datos$tipo_evento)
        datos$evento <- ParseaEsp(datos$evento)
        datos$medio_pago <- ParseaEsp(datos$medio_pago)
        datos$codigo_procedencia <- ParseaEsp(datos$codigo_procedencia)
        datos$canal_pago <- NULL
        datos$entidad <- ParseaEsp(datos$entidad)
        datos$nombre_pagador <- ParseaEsp(datos$nombre_pagador)
        datos$activa <- NULL
        
        fecha_fin <- Sys.time()
        write(paste(Sys.time()," - Peticion de: ",customer,", del dia: ",Dia_volcado," procesada en: ",round(fecha_fin-fecha_inicio)," segundos",sep=""),file=log_file,append=TRUE)
        return(datos)
      }else{
        write(paste(Sys.time(),' - GetVentas - Error de peticion (ErrorCode: 0x06 - No existen datos para la fecha solicitada), cliente: ',customer,', fecha: ',Dia_volcado,sep=""),file=log_file,append=TRUE)
        
        df <- data.frame(fecha = Sys.time(),
                         errorcode = "0x06",
                         errortxt = "No existen datos seleccionados para la fecha solicitada",
                         ayuda="Seleccione otra fecha")
        
        return(df) 
      }
      
    }, 
    error=function(cond) 
    { 
      write(paste(Sys.time(),' - GetVentas - Error de peticion (ErrorCode: 0x01 - Error de sistema interno)',sep=""),file=log_file,append=TRUE)
      
      df <- data.frame(fecha = Sys.time(),
                       errorcode = "0x01",
                       errortxt = "Error de sistema interno",
                       ayuda=paste("Esperamos dos parametros: fecha en formato aaaammdd y customer",sep=""))
      
      return(df) 
    } 
  ) 
}
