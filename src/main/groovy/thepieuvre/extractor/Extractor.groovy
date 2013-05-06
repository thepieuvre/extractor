package thepieuvre.extractor

import com.gravity.goose.Configuration
import com.gravity.goose.Goose
import de.l3s.boilerpipe.extractors.ArticleExtractor

import redis.clients.jedis.Jedis

import java.net.URL

import groovy.json.JsonBuilder
import groovy.json.JsonSlurper

class Extractor {
	private Configuration conf = new Configuration()
	private Goose goose
	private ArticleExtractor boilerpipe = ArticleExtractor.INSTANCE

	Extractor(){
		conf.enableImageFetching = true
		conf.imagemagickIdentifyPath= '/usr/local/bin/identify'
		conf.imagemagickConvertPath = '/usr/local/bin/convert'
		goose = new Goose(conf)
	}

	def goose(String link) throws Exception {
		def result = [:]
		result.extractor = 'Goose'
		def gExt = goose.extractContent(link)
		result.fullText = gExt.cleanedArticleText()
		result.mainImage = gExt.topImage.getImageSrc()
		return result
	}

	def boilerpipe(String link) throws Exception {
		def result = [:]
		result.extractor = 'Boilerpipe'
		result.fullText = boilerpipe.getText(new URL(link))
		return result
	}	

	def extracting(String link) throws Exception {
		def result = goose(link)
		if (result.fullText == null || result.fullText.isEmpty()) {
			def boilerpipe = boilerpipe(link)
			result.fullText = boilerpipe.fullText
			result.extractor = boilerpipe.extractor
		}
		return result
	}

	def redisMode() {
		println 'Starting listenning to the queue:extractor'
		Jedis jedis = new Jedis("localhost")
		jedis.sadd('queues', 'queue:article')
		while (true) {
			try {
				def task = jedis.blpop(31415, 'queue:extractor')
				if (task) {
					def decoded = new JsonSlurper().parseText(task[1])
					println "${new Date()} - Extracting content for $decoded.link"
					def extracted = extracting(decoded.link)
					def builder = new groovy.json.JsonBuilder()
					def root = builder.content {
						id decoded.id
						link decoded.link
						extractor extracted.extractor
						mainImage extracted.mainImage
						fullText extracted.fullText
					}
					jedis.rpush("queue:article", builder.toString())
					println "${new Date()} - Extracted and pushed to the queue:article"
				} else {
					continue
				}
			} catch (Exception e) {
				e.printStackTrace()
			}
		}
	}

	static void main(String [] args) {
		println "Starting extractor"
		Extractor extractor = new Extractor()

		if (args.size() != 1) {
			System.err.println("Not enought arguments")
			System.exit(1)
		}

		if (args[0] == '--redis-mode') {
			extractor.redisMode()
		} else {
			try {
				def goose = extractor.goose(args[0])
				def boilerpipe = extractor.boilerpipe(args[0])

				println goose
				println '--------------------'
				println boilerpipe
			} catch(e) {
				e.printStackTrace()
			}
		}

	}
}