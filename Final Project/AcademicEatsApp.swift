import SwiftUI

@main
struct AcademicEatsApp: App {
    @UIApplicationDelegateAdaptor(AcademicEatsDelegate.self) var appDelegate
    
    var body: some Scene {
        WindowGroup {
            ContentView()
                .environmentObject(AcademicEatsAuth())
                .environmentObject(AcademicEatsService())
        }
    }
}
