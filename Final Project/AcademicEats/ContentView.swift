import SwiftUI

struct ContentView: View {
    var body: some View {
        AcademicEats()
    }
}

struct ContentView_Previews: PreviewProvider {
    static var previews: some View {
        ContentView()
            .environmentObject(AcademicEatsAuth())
    }
}
