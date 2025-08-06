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
*                1. Random sample      *
************************************************
foreach y in 2018 2019 2020 2022 2023 2024{
	import delimited "$rawdata/population/Base Beneficiarios/Data Poblacion `y'.csv", clear varnames(1)
	if `y' <= 2020{
		keep benef_enciptado
	}
	else{
		keep código_beneficiario
		rename código_beneficiario benef_enciptado
	}
	destring benef_enciptado, replace
	tempfile b_`y'
	save `b_`y''
}
use `b_2018'
foreach y in 2019 2020 2022 2023 2024{
	append using `b_`y''
}
duplicates drop benef_enciptado, force
save "$usedata/benef_sample.dta"

************************************************
*         2. Sick leave data cleaning          *
************************************************