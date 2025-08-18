package com.optum.obhdp.tcoc.common

import java.nio.charset.StandardCharsets

import javax.crypto.spec.{PBEKeySpec, PBEParameterSpec}
import javax.crypto.{Cipher, SecretKeyFactory}
import org.apache.commons.codec.binary.Base64

object AesDesCrypt {
  private val ALGORITHM = "PBEWithMD5AndDES"
  // 8-byte Salt
  private val salt = Array(0xA9.toByte, 0x9B.toByte, 0xC8.toByte, 0x32.toByte, 0x56.toByte, 0x35.toByte, 0xE3.toByte, 0x03.toByte)

  // Iteration count
  private val iterationCount = 19

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    if(args.size < 3){
      println("Pass three arguments: <encrypt|decrypt> <value> <passphrase>\n" +
        "Example 1: com.optum.peds.dl.common.AesDesCrypt encrypt toencryptval TechopsPass\n" +
        "Example 2: com.optum.peds.dl.common.AesDesCrypt decrypt todecryptval TechopsPass" )
      System.exit(1)
    }
    val cryptmethod = args(0)
    val tocryptval:String = args(1)
    val password:String = args(2)

    if(cryptmethod == "encrypt"){
      val encryptValue = encrypt(password,tocryptval)
      println(encryptValue)
    }else if(cryptmethod == "decrypt") {
      val decryptValue = decrypt(password, tocryptval)
      println(decryptValue)
    }
  }

  /**
    *
    * @param valueToEnc
    * @throws Exception
    * @return
    */
  @throws[Exception]
  def encrypt(passPhrase: String, valueToEnc: String): String = {
    val key = generateKey(passPhrase)
    var ecipher = Cipher.getInstance(key.getAlgorithm)

    // Prepare the parameter to the ciphers
    val paramSpec = new PBEParameterSpec(salt, iterationCount)

    // Create the ciphers
    ecipher.init(Cipher.ENCRYPT_MODE, key, paramSpec)

    val encValue = ecipher.doFinal(valueToEnc.getBytes(StandardCharsets.UTF_8))
    val encryptedByteValue = new Base64().encode(encValue)
    val encryptedValue = new String(encryptedByteValue)
    encryptedValue
  }

  @throws[Exception]
  def decrypt(passPhrase: String, encryptedValue: String): String = {

    val key = generateKey(passPhrase)
    var dcipher = Cipher.getInstance(key.getAlgorithm)

    // Prepare the parameter to the ciphers
    val paramSpec = new PBEParameterSpec(salt, iterationCount)

    // Create the ciphers
    dcipher.init(Cipher.DECRYPT_MODE, key, paramSpec)

    val enctVal = dcipher.doFinal(new Base64().decode(encryptedValue.getBytes(StandardCharsets.UTF_8)))
    val decodedValue = new String(enctVal)
    decodedValue.toString
  }

  @throws[Exception]
  private def generateKey(passPhrase: String) = {
    val keySpec = new PBEKeySpec(passPhrase.toCharArray, salt, iterationCount)
    val key = SecretKeyFactory.getInstance(ALGORITHM).generateSecret(keySpec)
    key
  }

  import javax.crypto.spec.SecretKeySpec
  import java.security.Key

  @throws[Exception]
  private def generateKey2(passPhrase:String) = {
    val key = new SecretKeySpec(passPhrase.getBytes(StandardCharsets.UTF_8), ALGORITHM)
    key
  }
}