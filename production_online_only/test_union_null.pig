
A = load '/user/jli21/test/temp/three/dummy' using PigStorage(',') as (x,y,z);

B = load '/user/pythia/Workspaces/SamsMEP/scs_model.db/scs_purchase_model' using PigStorage(',') as (x,y,z);

C = union A, B;

dump C;



