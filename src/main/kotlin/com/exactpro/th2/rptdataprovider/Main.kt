import com.exactpro.th2.rptdataprovider.Context
import com.exactpro.th2.rptdataprovider.server.GrpcServer
import com.exactpro.th2.rptdataprovider.server.HttpServer
import io.ktor.server.engine.*
import io.ktor.util.*
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch


@FlowPreview
@EngineAPI
@InternalAPI
@ExperimentalCoroutinesApi
fun main(args: Array<String>) {
    val context = Context(args)
    GlobalScope.launch {
        HttpServer(context).run()
    }
    GlobalScope.launch {
        GrpcServer(context)
    }
}
