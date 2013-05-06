package thepieuvre.extractor

import com.gravity.goose.Configuration
import com.gravity.goose.Goose
import de.l3s.boilerpipe.extractors.ArticleExtractor

import java.net.URL

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

	static void main(String [] args) {
		println "Starting extractor"
		Extractor extractor = new Extractor()

		if (args.size() != 1) {
			System.err.println("Not enought arguments")
			System.exit(1)
		}
		try {
			def goose = extractor.goose(args[0])
			def boilerpipe = extractor.boilerpipe(args[0])

			println goose
			println '--------------------'
			println boilerpipe
		} catch(e) {
			println e
		}
	}
}