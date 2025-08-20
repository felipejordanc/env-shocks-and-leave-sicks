*********************************************
* Purpose: Sick leave database
*
* Date: 05/08/2025
*
* Author: Matías Black
*********************************************
clear all

************************************************
*                0. Key Macros                 *
************************************************

*Folder globals

di "current user: `c(username)'"
if "`c(username)'" == "black"{
	global path "C:/Users/black/Documents/GitHub/env-shocks-and-leave-sicks"
}



	global rawdata "$path/remote/data/A_raw"
	global usedata "$path/remote/data/B_intermediate"
	global final "$path/remote/data/C_final"
	global graphs "$path/graphs"
	global tables "$path/tables"
	
************************************************
*       1. Random sample                       *
************************************************
foreach y in 2018 2019 2020 2022 2023 2024{
	import delimited "$rawdata/population/Base Beneficiarios/Data Poblacion `y'.csv", clear varnames(1)
	if `y' <= 2020{
		keep benef_enciptado cod_comuna_pernat
	}
	else{
		keep código_beneficiario comuna_beneficiario
		rename código_beneficiario benef_enciptado
	}
	gen year = `y'
	tempfile b_`y'
	save `b_`y''
}
use `b_2018'
foreach y in 2019 2020 2022 2023 2024{
	append using `b_`y''
}
merge n:1 comuna_beneficiario using "C:\Users\black\Dropbox\GSL\Bases intermedias\codigo_comunas.dta", update nogenerate
drop comuna_beneficiario
rename cod_comuna_pernat cod_com
reshape wide cod_com, i(benef_enciptado) j(year)
save "$usedata/benef_sample.dta", replace // This one has to be deleted when making the replication file, it is just a checkpoint

gen all_years = cod_com2018 == cod_com2019 & cod_com2018 == cod_com2020 & cod_com2018 == cod_com2022 & cod_com2018 == cod_com2023 & cod_com2018 == cod_com2024
set seed 12345 
sample 1
keep benef_enciptado
save "$usedata/benef_sample1.dta", replace

************************************************
*         2. Population data cleaning          *
************************************************
foreach y in 2018 2019 2020{
	import delimited "$rawdata/population/Base Beneficiarios/Data Poblacion `y'.csv", clear varnames(1) 
	merge 1:1 benef_enciptado using "$usedata/benef_sample1.dta", nogenerate keep(3)
	gen sex = sexo_cor == "HOMBRE"
	gen carga = titular_carga == "CARGA"
	gen foreign = nacionalidad_cor == "EXTRANJERA"
	keep benef_enciptado edad_tramo cod_comuna_pernat tramo desc_tipo_asegurado densidad tramo_renta sex carga foreign
	gen year = `y'
	tempfile b_`y'
	save `b_`y''
}


foreach y in 2022 2023 2024{
	import delimited "$rawdata/population/Base Beneficiarios/Data Poblacion `y'.csv", clear varnames(1) 
	rename código_beneficiario benef_enciptado
	merge 1:1 benef_enciptado using "$usedata/benef_sample1.dta", nogenerate keep(3)
	gen sex = sexo == "Hombre"
	gen carga = titular_carga == "Carga"
	gen foreign = nacionalidad == "Extranjera"
	merge n:1 comuna_beneficiario using "C:\Users\black\Dropbox\GSL\Bases intermedias\codigo_comunas.dta", update nogenerate keep(3)
	rename tramo_fonasa tramo
	rename tipo_asegurado desc_tipo_asegurado
	keep benef_enciptado edad_tramo cod_comuna_pernat tramo desc_tipo_asegurado tramo_renta sex carga foreign
	gen year = `y'
	tempfile b_`y'
	save `b_`y''
}
use `b_2018'
foreach y in 2019 2020 2022 2023 2024{
	append using `b_`y''
}
order benef_enciptado year
sort benef_enciptado year
replace edad_tramo = "0 a 9 años" if edad_tramo == "00 a 02 años" | edad_tramo == "00 a 4 años" | edad_tramo == "03 a 04 años" | edad_tramo == "05 a 09 años" 
replace edad_tramo = "10 a 19 años" if edad_tramo == "10 a 14 años" | edad_tramo == "15 a 19 años" 
replace edad_tramo = "20 a 29 años" if edad_tramo == "20 a 24 años" | edad_tramo == "25 a 29 años"
replace edad_tramo = "30 a 39 años" if edad_tramo == "30 a 34 años" | edad_tramo == "35 a 39 años" 
replace edad_tramo = "40 a 49 años" if edad_tramo == "40 a 44 años" | edad_tramo == "45 a 49 años" 
replace edad_tramo = "50 a 59 años" if edad_tramo == "50 a 54 años" | edad_tramo == "55 a 59 años" 
replace edad_tramo = "60 a 69 años" if edad_tramo == "60 a 64 años" | edad_tramo == "65 a 69 años" 
replace edad_tramo = "70 a 79 años" if edad_tramo == "70 a 74 años" | edad_tramo == "75 a 79 años" 
replace edad_tramo = "80 a más años" if edad_tramo == "80 a 84 años" | edad_tramo == "85 a 89 años" | edad_tramo == "90 a 94 años" | edad_tramo == "95 a 99 años" | edad_tramo == "Más de 99 años" 
replace edad_tramo = "NA" if edad_tramo == "S.I." | edad_tramo == "Sin información" 
replace desc_tipo_asegurado = "Carente" if desc_tipo_asegurado == "CARENTE"
replace desc_tipo_asegurado = "Desconocido" if desc_tipo_asegurado == "DESCONOCIDO"
replace desc_tipo_asegurado = "Pensionado" if desc_tipo_asegurado == "PENSIONADO"
replace desc_tipo_asegurado = "Trabajador Dependiente" if desc_tipo_asegurado == "TRABAJADOR DEPENDIENTE"
replace desc_tipo_asegurado = "Trabajador Independiente" if desc_tipo_asegurado == "TRABAJADOR INDEPENDIENTE"

save "$usedata/Population.dta", replace

************************************************
*         3. Employer data cleaning          *
************************************************
local mydir "C:\Users\black\Documents\Plantillas personalizadas de Office\OneDrive_6_7-11-2025_0_100\LT8696_TEST_999_20250710\"
local files : dir "`mydir'" files "*.txt"
local i = 1
foreach f of local files {
	import delimited "`mydir'\`f'", varnames(1) clear
	rename codigo_cotizante benef_enciptado
	merge n:1 benef_enciptado using "$usedata/benef_sample1.dta", nogenerate keep(3)
	tostring periodo_renta, replace
	replace periodo_renta = substr(periodo_renta,1,4) + "m" + substr(periodo_renta,5,2)
	gen date = monthly(periodo_renta, "YM")
	format date %tm
	drop actividad_economica periodo_renta
	tempfile e_`i'
	save `e_`i''
	local ++i
}
use `e_1'
forvalues y = 2/1000{
	append using `e_`y''
}
save "$usedata/Employer.dta", replace
************************************************
*         4. Sick leave data cleaning          *
************************************************
import delimited "$rawdata/sick leaves\T8314.csv", clear varnames(1)
preserve
gen suma = 1
collapse (sum) suma, by(rut_prof_encriptado)
save "$usedata/Doctors.dta", replace
restore
rename encripbi_rut_traba benef_enciptado
merge n:1 benef_enciptado using "$usedata/benef_sample1.dta", nogenerate keep(3)
gen date = daily(fecha_emision, "DMY")
format date %td
keep desc_tipo_formulario benef_enciptado date dias_otorgados rut_prof_encriptado cod_tipo_licencia coddersubsidio codautorizacion lic_cod_previsional
save "$usedata/Sick.dta", replace

************************************************
*         5. Main data cleaning          *
************************************************

clear all
set obs 1
gen date = mdy(1,1,2018)
format date %td
gen enddate = mdy(12,31,2024)
format enddate %td
expand enddate - date + 1
replace date = date[1] + _n +-1
drop enddate
tempfile dates
save `dates'

use "$usedata/benef_sample1.dta", clear
cross using `dates'
sort benef_enciptado date
gen leave = 0
gen year = year(date)
merge 1:1 benef_enciptado date using "$usedata/Sick.dta", nogenerate keep (1 3)
gen enddate = .
replace enddate = date + dias_otorgados - 1 if dias_otorgados < .
bysort benef_enciptado (date): replace enddate = enddate[_n-1] if enddate[_n-1] > date[_n-1] & enddate[_n-1] != . & enddate == .
replace leave = -98 if enddate != . & leave != 1
drop enddate
use "$usedata/Main.dta", clear

* Año-mes en formato monthly (%tm)
*gen ym = mofd(datevar)
*format ym %tm
save "$usedata/Main.dta", replace
















