package org.apache.spark.tez.instrument

import javassist.ClassPool
import javassist.CtClass
import javassist.CtMethod
import sun.misc.Unsafe
import java.util.HashMap
import javassist.ClassMap
import javassist.Modifier
import javassist.CtField
import org.apache.tez.dag.api.TezConfiguration
import java.net.URLClassLoader
import javassist.ClassPath
import javassist.LoaderClassPath

/**
 * 
 */
object TezInstrumentationAgent {

  val unsafe = {
    val field = classOf[Unsafe].getDeclaredField("theUnsafe");
    field.setAccessible(true);
    field.get(null).asInstanceOf[Unsafe]
  }
  
  private val pool = ClassPool.getDefault();
  val cp = new LoaderClassPath(Thread.currentThread().getContextClassLoader())
  pool.childFirstLookup = true
  pool.insertClassPath(cp)

  val cl3 = pool.getClassLoader().asInstanceOf[URLClassLoader]
  println("Classpath available to JAVASSIST instrumentation")
  println(cl3.getURLs().toList)
  println()
  
  private val tezContextClass = pool.get("org.apache.spark.tez.TezContext")
  private val sparkContextClass = pool.get("org.apache.spark.SparkContext")
  
  // This block will finally replace all references to 'org.apache.spark.tez.TezContext' with 'org.apache.spark.SparkContext'
  // to finalize SparkContext instrumentation
  // java.lang.NoSuchMethodError: org.apache.spark.tez.TezContext$$anonfun$textFile$1.<init>(Lorg/apache/spark/SparkContext;)V
  tezContextClass.getNestedClasses().foreach{
    x => x.replaceClassName("org.apache.spark.tez.TezContext", "org.apache.spark.SparkContext"); 
    x.toClass()
  }
  
  /**
   * 
   */
  def instrument = {
    // this code will be plugged in when ready to override primary constructor to avoid Spark's garbage not required by Tez
//    val baseConstructor = this.sparkContextClass.getConstructor("(Lorg/apache/spark/SparkConf;)V")
//    val instrConstructor = this.tezContextClass.getConstructor("(Lorg/apache/spark/SparkConf;)V")
//    baseConstructor.setBody(instrConstructor, null)
    
    val targetMethods = sparkContextClass.getDeclaredMethods
    for (targetMethod <- targetMethods){
      if (targetMethod.getName() == "parallelize"){
        this.swapMethodBody(targetMethod)
      }
      else if (targetMethod.getName() == "newAPIHadoopFile"){
        this.swapMethodBody(targetMethod)
      }
      else if (targetMethod.getName() == "textFile"){
        this.swapMethodBody(targetMethod)
      }
      else if (targetMethod.getName() == "runJob"){
        this.swapMethodBody(targetMethod)
      }
//      else if (targetMethod.getName() == "defaultParallelism"){
//        this.swapMethodBody(targetMethod)
//      }
    }

    val scBytes = this.sparkContextClass.toBytecode()
    unsafe.defineClass(null, scBytes, 0, scBytes.length, this.getClass.getClassLoader(), this.getClass.getProtectionDomain())
  }

  /**
   *
   */
  private def swapMethodBody(targetMethod: CtMethod) {
    val desc = targetMethod.getMethodInfo().getDescriptor()
    try {
      val sourceMethod = tezContextClass.getMethod(targetMethod.getName(), desc)
      targetMethod.setBody(sourceMethod, null)
    } catch {
      case e: Throwable => throw new IllegalStateException(e);
    }
  }
}