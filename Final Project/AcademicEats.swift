import SwiftUI

struct AcademicEats: View {
    @EnvironmentObject var auth: AcademicEatsAuth
    @State var requestLogin = false
    
    var body: some View {
        if let authUI = auth.authUI {
            TabBar(requestLogin: $requestLogin)
                .sheet(isPresented: $requestLogin) {
                    AuthenticationViewController(authUI: authUI)
                }
        } else {
            VStack {
                Text("Sorry, looks like we aren’t set up right!")
                    .padding()
                Text("Please contact this app’s developer for assistance.")
                    .padding()
            }
        }
    }
}

struct AcademicEats_Previews: PreviewProvider {
    static var previews: some View {
        AcademicEats().environmentObject(AcademicEatsAuth())
    }
}
