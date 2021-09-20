package pollnpull

type Developer struct {
	ID            string `json:"id,omitempty"`
	FullName      string `json:"full_name,omitempty"`
	Organization  string `json:"organization,omitempty"`
	ContactHandle string `json:"contact_handle,omitempty"`
}