SELECT distinct
         [State]
       ,pt.[PatientNumber]
      ,[CptCode]
      ,[CptDesc]
      --,[CptGrouping]
      ,pn.ProviderNpi
      ,pn.ProviderSpecialty
      ,ln.LocationName
      ,py.PayerName
      ,tn.[Transaction]
      ,ft.[CPTUnits]
      ,ft.[GrossCharge]
      ,ft.[Payment]
      ,ft.[Adjustment]
      ,ft.[AR]
FROM FactTable ft
   LEFT JOIN [dimCptCode] cpt ON  ft.dimCPTCodePK = cpt.dimCPTCodePK
   LEFT JOIN dimPhysician pn ON ft.dimPhysicianPK = pn.dimPhysicianPK
   LEFT JOIN dimLocation ln ON ft.dimLocationPK = ln.dimLocationPK
   LEFT JOIN dimPayer py ON ft.dimPayerPK = py.dimPayerPK
   LEFT JOIN [dimTransaction] tn ON  ft.dimTransactionPK = tn.dimTransactionPK 
   LEFT JOIN [dbo].[dimPatient] pt ON  ft.dimPatientPK = pt.dimPatientPK
   
WHERE [State] ='AL' 
--AND cpt.CptCode='3079F' 
AND pt.PatientNumber ='21364039'
--AND tn.dimTransactionPK ='52998' ORDER BY [State], PatientNumber 