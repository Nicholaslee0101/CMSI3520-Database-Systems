import Foundation

import FirebaseAuthUI
import FirebaseEmailAuthUI

class AcademicEatsAuth: NSObject, ObservableObject, FUIAuthDelegate {
    let authUI: FUIAuth? = FUIAuth.defaultAuthUI()
    
    let providers: [FUIAuthProvider] = [
        FUIEmailAuth()
    ]
    
    @Published var user: User?
    
    override init() {
        super.init()
        
        authUI?.delegate = self
        authUI?.providers = providers
    }
    
    func authUI(_ authUI: FUIAuth, didSignInWith authDataResult: AuthDataResult?, error: Error?) {
        if let actualResult = authDataResult {
            user = actualResult.user
        }
    }
    
    func signOut() throws {
        try authUI?.signOut()
        // If we get past the logout attempt, we can safely clear the user.
        user = nil
    }
}
