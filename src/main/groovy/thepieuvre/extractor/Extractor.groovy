package thepieuvre.extractor

import com.cybozu.labs.langdetect.Detector
import com.cybozu.labs.langdetect.DetectorFactory
import com.gravity.goose.Configuration
import com.gravity.goose.Goose
import de.l3s.boilerpipe.extractors.ArticleExtractor

import redis.clients.jedis.Jedis

import java.net.URL

import groovy.json.JsonBuilder
import groovy.json.JsonSlurper

class Extractor {

	def printErr = System.err.&println

	private Configuration conf = new Configuration()
	private Goose goose
	private ArticleExtractor boilerpipe = ArticleExtractor.INSTANCE

	private static def repushed = [:]

	Extractor(File profileDirectory){
		conf.enableImageFetching = true
		conf.imagemagickIdentifyPath= '/usr/local/bin/identify'
		conf.imagemagickConvertPath = '/usr/local/bin/convert'
		goose = new Goose(conf)
		DetectorFactory.loadProfile(profileDirectory)
	}

	def goose(String link) throws Exception {
		def result = [:]
		result.extractor = 'Goose'
		def gExt = goose.extractContent(link)
		result.fullText = gExt.cleanedArticleText()
		result.mainImage = gExt.topImage.getImageSrc()
		return result
	}

	def boilerpipe(String text) throws Exception {
		def result = [:]
		result.extractor = 'Boilerpipe'
		result.fullText = boilerpipe.getText(text)
		return result
	}	

	def extracting(String link) throws Exception {
		def result = [:]
		try {
			result = goose(link)
		} catch (Exception e) {
			 printErr("${new Date()} - Exception from goose ${e.getMessage()}")
		}
		if (result.fullText == null || result.fullText.isEmpty()) {
			def boilerpipe = boilerpipe(result.rawHtml)
			result.fullText = boilerpipe.fullText
			result.extractor = boilerpipe.extractor
		}
		return result
	}

	def guessLang(String text) {
		try {
			Detector detector = DetectorFactory.create()
			detector.append(text)

			return detector.detect()
		} catch (com.cybozu.labs.langdetect.LangDetectException e) {
			println "No text for guessing"
			return 'no-lang'
		}
	}

	def redisMode() {
		println 'Starting listenning to the queue:extractor'
		Jedis jedis = new Jedis("localhost")
		jedis.sadd('queues', 'queue:article')
		while (true) {
			def task 
			def decoded
			try {
				task = jedis.blpop(31415, 'queue:extractor')
				if (task) {
					decoded = new JsonSlurper().parseText(task[1])
					println "${new Date()} - Extracting content for $decoded.link"
					def extracted = extracting(decoded.link)
					def guessLang = (extracted?.fullText)?guessLang(extracted.fullText):guessLang(decoded.raw)
					def builder = new groovy.json.JsonBuilder()
					def root = builder.content {
						id decoded.id
						link decoded.link
						extractor extracted.extractor
						mainImage extracted.mainImage
						fullText extracted.fullText
						lang guessLang
					}
					jedis.rpush("queue:article", builder.toString())
					println "${new Date()} - Extracted and pushed to the queue:article: $decoded.id / $guessLang"
				} else {
					continue
				}
			} catch (de.l3s.boilerpipe.BoilerpipeProcessingException e) {
				if (task && decoded) {
					if (repushed[decoded.link]) {
						println "${new Date()} - No content ${e.getMessage()} -- already repushed / remove it"
						repushed.remove(decoded.link)
					} else {
						println "${new Date()} - No content ${e.getMessage()} -- repushed to the queue"
						repushed[decoded.link] = new Date()
						jedis.rpush("queue:extractor", task[1])
					}
				} else {
					println "${new Date()} - No content ${e.getMessage()}"
				}
			} catch (Exception e) {
				printErr("${new Date()} - Problem with ${decoded}. Exception below:")
				e.printStackTrace()
			}
		}
	}

	static void main(String [] args) {
		println "Starting extractor"

		if (args.size() != 2) {
			System.err.println("Not enought arguments")
			System.exit(1)
		}

		Extractor extractor = new Extractor(new File(args[1]))

		if (args[0] == '--redis-mode') {
			extractor.redisMode()
		} else {
			try {
				def goose = extractor.goose(args[0])
				def boilerpipe = extractor.boilerpipe(goose.rawHtml)
				def lang = extractor.guessLang((goose.fullText)?:boilerpipe.fullText)

				println goose
				println '--------------------'
				println boilerpipe
				println '--------------------'
				println lang
			} catch(e) {
				printErr("${new Date()} - Exception below:")
				e.printStackTrace()
			}
		}

	}
}