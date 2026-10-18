import com.sun.tools.attach.VirtualMachine;
import java.lang.management.ManagementFactory;
import javax.management.ObjectName;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

/** Own-user local attach; no public JMX port or broker write. Invoked only by Jenkins. */
class CvdMirrorReadiness {
    static void checkHeap(java.lang.management.MemoryUsage heap) {
        if(heap.getMax()!=256L*1024*1024)throw new IllegalStateException("Configured heap cap differs");
    }
    public static void main(String[] args) throws Exception {
        if(args.length!=3||!args[0].matches("[1-9][0-9]*"))throw new IllegalArgumentException("pid/topic/group required");
        var vm=VirtualMachine.attach(args[0]);String address;
        try {address=vm.startLocalManagementAgent();} finally {vm.detach();}
        try(var connector=JMXConnectorFactory.connect(new JMXServiceURL(address))) {
            var server=connector.getMBeanServerConnection();
            var name=new ObjectName("optionsedge.mirror:type=Transport,topic="+ObjectName.quote(args[1])+",group="+ObjectName.quote(args[2]));
            if(!Boolean.TRUE.equals(server.getAttribute(name,"Ready"))||((Number)server.getAttribute(name,"Failures")).longValue()!=0)
                throw new IllegalStateException("Mirror is not ready or has failed");
            if(((Number)server.getAttribute(name,"LastPollNanos")).longValue()<=0)throw new IllegalStateException("Mirror has not polled");
            var memory=ManagementFactory.newPlatformMXBeanProxy(server,ManagementFactory.MEMORY_MXBEAN_NAME,java.lang.management.MemoryMXBean.class);
            checkHeap(memory.getHeapMemoryUsage());
            System.out.println("CANONICAL_MIRROR_READY records="+server.getAttribute(name,"Records"));
        }
    }
}
