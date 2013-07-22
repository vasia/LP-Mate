package org.apache.pig.newplan.optimizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import org.apache.pig.FuncSpec;
import org.apache.pig.LoadFunc;

import org.apache.pig.ResourceSchema;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.newplan.optimizer.Repository;


import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.io.PrintWriter;
import java.io.FileWriter;
import java.io.BufferedWriter;


import org.apache.pig.builtin.PigStorage;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.PlanEdge;
import org.apache.pig.newplan.logical.Util;
import org.apache.pig.newplan.optimizer.Rule;

import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LOJoin;
import org.apache.pig.newplan.logical.relational.LOCogroup;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LOStore;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.logical.relational.LogicalSchema;

import java.util.ArrayList;
import java.util.Set;
import java.util.Date;
import java.text.SimpleDateFormat;

import org.apache.pig.newplan.logical.rules.ImplicitSplitInserter;

import com.mysql.clusterj.Session;

public class PlanMatcher {

protected LogicalPlan plan;
LogicalPlan generatePlan;
Session session;
FuncSpec func;
public PlanMatcher(LogicalPlan p, Session sess){
  plan=p;
	session=sess;
        func=((LOStore)(p.getSinks().get(0))).getFileSpec().getFuncSpec();
}
public void test(){
	Repository test = session.find(Repository.class,"shang");
	if (test==null) System.out.println("Program didn't find it");
	else{
		System.out.println("Progarm find shang and"+ test.getFilename());
	}
		
}
public boolean check(Repository plan) throws FrontendException{
	if (plan==null) return false;
	return true;
}
public List<Set<Rule>>  getRule(){
	 Set<Rule> s = new HashSet<Rule>();
     List<Set<Rule>> ls = new ArrayList<Set<Rule>>();
	 Rule r = new ImplicitSplitInserter("ImplicitSplitInserter");
	
     s.add(r);
     ls = new ArrayList<Set<Rule>>();
     ls.add(s);

	
     return ls;
}
public void saveToRepo(String hash, String name){
        if(name!="fakefile"){	
	Repository savePlan = session.newInstance(Repository.class);
	savePlan.setHashcode(hash);	
	savePlan.setFilename(name);
	session.savePersistent(savePlan);}
}

/*public String getName(String name) throws IOException{
	Configuration conf = new Configuration();
	FileSystem fs = FileSystem.get(conf);
	String result=null;
	Path path= new Path(name);
	 if (fs.exists(path)) {
		 //FSDataInputStream in = fs.open(path);
		 FileStatus[] files = fs.listStatus(path);
                 
                  for (FileStatus file:files){
			 String filename=file.getPath().getName();
                         
			 if(filename.startsWith("part"))
				{result= name+"/"+filename;
                        System.out.println("filename "+result);
                        break;}
	}
	 }
	 return result;
}*/
public String getName(String name) throws IOException{
	Configuration conf = new Configuration();
	FileSystem fs = FileSystem.get(conf);
	String result=null;
	Path path= new Path(name);
	/* if (fs.exists(path)) {
		 //FSDataInputStream in = fs.open(path);
		 FileStatus[] files = fs.listStatus(path);
		 for (FileStatus file:files){
			 String filename=file.getPath().getName();
             
			 if(filename.startsWith("part"))
				{result= name+"/"+filename;System.out.println("filename "+result);break;}
	 }
	 }*/
	 if (fs.exists(path)) {
		 //FSDataInputStream in = fs.open(path);
                Path path1=new Path(name+"/reuse");
             if(fs.exists(path1))result= name+"/reuse"; 
            else{
		 result=name+"/reuse";
                 fs.mkdirs(path1); 
                 FileStatus[] files = fs.listStatus(path);
                 
			
	 	 for (FileStatus file:files){
			 String filename=file.getPath().getName();
                        // System.out.println(filename);
                         if(filename.startsWith("part")){
                         String s1= name+"/"+filename;
                         String s2= result+"/"+filename;
                         Path inFile=new Path(s1);
                        // System.out.println("infile"+inFile);
                         Path outFile=new Path(s2);
                       //  System.out.println("outfile"+outFile);
                        // FSDataInputStream in= fs.open(inFile);
                        // FSDataOutputStream out=fs.create(outFile);
                        // byte[] b = new byte[1024];
                        // int numBytes = 0;
                        // while ((numBytes = in.read(b)) > 0) {
                          //   out.write(b, 0, numBytes);
                       // }
                       // in.close();
                       // out.close();
                         fs.rename(inFile,outFile);
                       }
                      }
                         
	 }
         }
        
	 return result;
}

public String getInput(LogicalPlan p){
	List<Operator> roots = p.getSources();
	StringBuilder sb=new StringBuilder();
	for(Operator op:roots){
		if(op instanceof LOLoad){
			sb.append("_");
			sb.append(((LOLoad) op).getFileSpec().getFileName());
		}
	}
	return Integer.toString(sb.toString().hashCode());
}
/*public String getInput(LogicalPlan p){
	List<Operator> roots = p.getSources();
	StringBuilder sb=new StringBuilder();
	List<String> orderName=new ArrayList<String>();
	for(Operator op:roots){
		if(op instanceof LOLoad){
			String s= ((LOLoad) op).getFileSpec().getFileName();
			orderName.add(s);
		}
	}
	Collections.sort(orderName);
	for(String name: orderName){
		sb.append("_");
		sb.append(name);
	}
	return sb.toString();
}*/
public int getCount(int jump){
	int count=1;
	if (jump==1)count= 2;
	else{
		List<Operator> l=plan.getOpList();
		while((count<l.size())){
			if (plan.getPredecessors(l.get(count)).size()<2)count++;
                        else break;
		}
		count=count+1;
	}
	return count;
}
public LogicalPlan matchAndRewrite() throws IOException{
	String newInputName=null;
	List<Operator> leaves = plan.getSinks();
        int loadnum=plan.getSources().size();
	LogicalRelationalOperator storeOp=(LogicalRelationalOperator) leaves.get(0);
         String filename=((LOStore) storeOp).getOutputSpec().getFileName();        
        long startTime1 =  System.currentTimeMillis();
    String planhash=plan.getSignature();
    long startTime2 =  System.currentTimeMillis();
	String hash=planhash+"_"+getInput(plan);
	long startTime3 =  System.currentTimeMillis();
	Repository theplan = session.find(Repository.class,hash);
 	long startTime4 =  System.currentTimeMillis();
        long hashtime=startTime2-startTime1;
 	long checktime=startTime4-startTime3;
       
	//PrintWriter writer = new PrintWriter("/home/ubuntu/save",true);
	PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter("/home/ubuntu/save",true)));
    writer.println("hashtime	"+hashtime);
	writer.println("checktime	"+checktime);
       writer.close();
	int count=getCount(loadnum);
	// this plan is not in the repository, save the hash and output of the plan
	if(leaves.size()==1){
        
	if(!check(theplan)){
		 PrintWriter writer1 = new PrintWriter(new BufferedWriter(new FileWriter("/home/ubuntu/save",true)));
		long startTime5 = System.currentTimeMillis();
		saveToRepo(hash,filename);
		long startTime6 = System.currentTimeMillis();
		long writeRepotime=startTime6-startTime5;
		writer1.println("writeRepotime	"+writeRepotime);
		writer1.close();
		String subfilename=filename+"_";
                        
                //String subfilename=filename+"/";
                generatePlan=generateSub(plan,getOpE(),subfilename,count);	
	}
	//this plan is in the repository, read the output, rewrite the plan
	else{	
		try {
		        PrintWriter writer5 = new PrintWriter(new BufferedWriter(new FileWriter("/home/ubuntu/save",true)));
                long startTime10 = System.currentTimeMillis();      
                 	newInputName=getName(theplan.getFilename());
                    long startTime11 = System.currentTimeMillis();
                    long readRepotime=startTime11-startTime10;
                   writer5.println("readRepotime  "+readRepotime);
                writer5.close();

		} catch (Throwable e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if(newInputName==null){
              theplan.setFilename(filename);   
	session.updatePersistent(theplan);
		String subfilename=filename+"_";
                // String subfilename=filename+"/";
                        generatePlan=generateSub(plan,getOpE(),subfilename,count);		
                 }	
	
                else{
		
	       updateRepo(theplan);
		LogicalSchema s=storeOp.getSchema();
		plan.disconnect(plan.getPredecessors(storeOp).get(0),storeOp);      
		generatePlan=rewriteWhole(newInputName,storeOp,s);
	
        }
	}
	}
       else{
        
        // saveToRepo(hash,filename);
        generatePlan=plan; 
       /* if(!check(theplan))
		{
			saveToRepo(hash,filename);
			generatePlan=plan;
		}
		else{
				try {
					newInputName=getName(theplan.getFilename());

				} catch (Throwable e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				if(newInputName==null){
		              theplan.setFilename(filename);   
			session.updatePersistent(theplan);
		                        generatePlan=plan;		
		                 }	
			
		                else{
				
			       updateRepo(theplan);
				LogicalSchema s=storeOp.getSchema();
				plan.disconnect(plan.getPredecessors(storeOp).get(0),storeOp);      
				generatePlan=rewriteWhole(newInputName,storeOp,s);
			
		        }
			}*/



      }    
 	return generatePlan; 
}

public LogicalPlan generateSub(LogicalPlan p,Set<String> s, String fname, int count) throws IOException{
	
	 List<Operator> ls=new ArrayList<Operator>();
	 List<Operator> opRemove= new ArrayList<Operator>();
	List<Operator> l=p.getOpList();
	//int count=2;
  // System.out.println("count"+count);
    while(count<l.size()){ 
	Operator op=l.get(count);
	//	System.out.println("l size"+""+l.size());        	
          //if(s.contains(opName))
               //if ((op instanceof LOFilter) || (op instanceof LOCogroup) || (op instanceof LOJoin) || (op instanceof LOForEach))
        //if(plan.getSuccessors(node).size()>0
         if(storeNode(op))
         {
			//String operatorHash=((LogicalRelationalOperator)op).getOpHashString()+getInput(p);
			String operatorHash=((LogicalRelationalOperator)op).getOpHashString();
			//modify String outName=fname+"_"+op.getName();
                  String outName=fname+op.getName()+"_"+operatorHash;
		/*	LOStore newStoreOp=createStoreOp(outName,p);
		 p.connect(op, newStoreOp);
		 saveToRepo(operatorHash,outName);
		 ls.add(newStoreOp);*/
        //new
        List<Operator> thisopRemove=new ArrayList<Operator>();
        //new
        if( matchSub(l,thisopRemove,ls,op,p,operatorHash,outName)) //modify count=count+opRemove.size()-1;	
          {     
                //System.out.println("true size");
                //System.out.println(thisopRemove.size());
        	count=count+thisopRemove.size()-1;
               // System.out.println("count"+""+count);
        	opRemove.addAll(thisopRemove);
        }
		}
        
	count=count+1;
        //System.out.println("count+1"+""+count);
}


for(int i=0;i<opRemove.size();i++){
	p.remove(opRemove.get(i));
}

for(int i=0;i<ls.size();i++){
	p.add(ls.get(i));
	//System.out.println("opadd"+ls.get(i));
}

/*try {p.getSignature();}
catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
Iterator<Operator> test=p.getOperators();
    while(test.hasNext()){ 
	Operator optest=test.next();
		
System.out.println(optest.getName());
		System.out.println("hash,,,,,,,,,,,,,,,,,,,,,,");
		System.out.println(((LogicalRelationalOperator)optest).getOpHashString());
		System.out.println("hash,,,,,,,,,,,,,,,,,,,,,,");
}
*/
/*PlanOptimizer optimizer = new MyOptimizer( p, getRule(), 3);


	try {
		optimizer.optimize();
	} catch (FrontendException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}*/





return p;
}
public boolean storeNode(Operator node){
           
           // if(node instanceof LOForEach) return true;
           // if(node instanceof LOFilter)  return true;
            if(node instanceof LOJoin) return true;
           if(node instanceof LOCogroup) return true;
         //  if((node instanceof LOFilter)&&(!(nodeSucc instanceof LOForEach)))  return true;
           // if((node instanceof LOJoin)&&(!(nodeSucc instanceof LOForEach))) return true;
           // if((node instanceof LOCogroup)&&(!(nodeSucc instanceof LOForEach))) return true;
            return false;
            
    }
public boolean matchSub(List<Operator> opList,List<Operator> opToRemove,List<Operator> opToAdd, Operator op,LogicalPlan p, String hashOfOp,String outputOfOp) throws IOException{
	Repository planToCheck = session.find(Repository.class,hashOfOp);
	if(!check(planToCheck)){
		
		LOStore newStoreOp=createStoreOp(outputOfOp,p);
		 p.connect(op, newStoreOp);
		 saveToRepo(hashOfOp,outputOfOp);
		 opToAdd.add(newStoreOp);
		 return false;
	}
	else{
		String newname=getName(planToCheck.getFilename());
		if(newname==null){
			planToCheck.setFilename(outputOfOp);
			session.updatePersistent(planToCheck);
			LOStore newStoreOp=createStoreOp(outputOfOp,p);
			 p.connect(op, newStoreOp);
			 opToAdd.add(newStoreOp);
			 return false;
		}	
		
		else{
		/*updateRepo(planToCheck);

                LogicalSchema ls=((LogicalRelationalOperator)op).getSchema();
		LOLoad load=createLoadOp(newname,ls, p);
	   // List<Operator> rm=opList.subList(opList.indexOf(op),opList.size());
	 getOpToRemove(opToRemove,p,op);
	
	Operator succ=p.getSuccessors(op).get(0);	   
	 PlanEdge pf=p.getFromEdges();
	    PlanEdge pt=p.getToEdges();
	   for(int i=0;i<opToRemove.size();i++ )
	   {    Operator oprm=opToRemove.get(i);
		pf.removeKey(oprm);
	   pt.removeKey(oprm);}
	  pt.removeKey(succ);
	   p.setFromEdges(pf);
	   p.setToEdges(pt);
	   
	p.connect(load,succ);
	   opToAdd.add(load);
	
	   return true;*/
    getOpToRemove(opToRemove,p,op);	
	if(opToRemove.size()==0)
		return false;
	else{
	    updateRepo(planToCheck);
            //System.out.println("op"+op.getName());
                LogicalSchema ls=((LogicalRelationalOperator)op).getSchema();
		LOLoad load=createLoadOp(newname,ls, p);
	   // List<Operator> rm=opList.subList(opList.indexOf(op),opList.size());
	// load.resetUid();
	//System.out.println("succsize");
       // System.out.println(p.getSuccessors(op).size());
    	Operator succ=p.getSuccessors(op).get(0);	   
      //  List<Operator> suc =p.getSuccessors(op);     
       //	p.disconnect(op,succ);
            PlanEdge pf=p.getFromEdges();
	    PlanEdge pt=p.getToEdges();
	  
         // for(Operator oprm:opToRemove )
	 for(int i=1;i<opToRemove.size();i++) 
          {        
               Operator oprm=opToRemove.get(i);
		pf.removeKey(oprm);
	   pt.removeKey(oprm);}

           pt.removeKey(op);
           // pt.removeKey(succ);
	  	// for(Operator succ :suc) 
		// pt.removeKey(succ);
         p.disconnect(op,succ);
           p.setFromEdges(pf);
	   p.setToEdges(pt);
	   
       p.connect(load,succ);
        // for(Operator succ1 :suc)
//	p.connect(load,succ1);
	   opToAdd.add(load);
	
	   return true;
     }
        }
	}
}

/*public void getOpToRemove(List<Operator> lrm,LogicalPlan p, Operator op){
	lrm.add(op);
	List<Operator> preds=p.getPredecessors(op);
	if (preds==null) return;
		for(Operator pred:preds){
		    getOpToRemove(lrm,p,pred);}	

}*/

public void getOpToRemove(List<Operator> lrm,LogicalPlan p, Operator op){
	lrm.add(op);
	List<Operator> preds=p.getPredecessors(op);
	if (preds==null) return;
		for(Operator pred:preds){
			if(p.getSuccessors(pred).size()>1) {
				lrm.clear();
				break;
				}
			else{
		    getOpToRemove(lrm,p,pred);
		    }
			}	

}

public Set<String> getOpE(){
	 Set<String> s = new HashSet<String>();
	 s.add("LOCogroup");
	 s.add("LOJoin");
	 s.add("LOFilter");
	 return s;
}
public LogicalPlan rewriteWhole(String name, Operator sop, LogicalSchema s) throws FrontendException{


	    //create a new logical plan with load and store
		LogicalPlan newWholePlan=new LogicalPlan();
	    LOLoad load=createLoadOp(name,s,newWholePlan);
        newWholePlan.add(load);
        
        sop.setPlan(newWholePlan);
       newWholePlan.add(sop);
       newWholePlan.connect(load, sop);
		return newWholePlan;
        
}
public void updateRepo(Repository p){
	p.setFrequency(p.getFrequency()+1); 
	String date=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
	p.setLast_access(date);
	session.updatePersistent(p);
}
public LOLoad createLoadOp(String inputName,LogicalSchema lschema, LogicalPlan plan) throws FrontendException{
	FileSpec loader= new FileSpec(inputName,func);
	LoadFunc l = (LoadFunc) PigContext.instantiateFuncFromSpec(loader.getFuncSpec()); 
	Configuration conf = new Configuration();
       if(lschema==null)
	{
	 try {
		ResourceSchema schema = Utils.getSchema(l,inputName,true, new Job(conf));
		Schema sschema=Schema.getPigSchema(schema);
		lschema=Util.translateSchema(sschema);
      
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
 	}
   LOLoad load = new LOLoad(loader,lschema,plan,conf,l,null);
//System.out.println("load"+lschema);
//System.out.println(load.getSchema());
   return load;
}
public LOStore createStoreOp(String outputName,LogicalPlan plan){
//	FuncSpec func = new FuncSpec(PigStorage.class.getName());
	FileSpec outFileSpec= new FileSpec(outputName,func);
 StoreFuncInterface storeFunc = (StoreFuncInterface)PigContext.instantiateFuncFromSpec(func); 	
 LOStore newStoreOp=new LOStore(plan,outFileSpec,storeFunc,null);
 return newStoreOp;
}

}
